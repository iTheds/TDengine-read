/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

#define _DEFAULT_SOURCE
#include "tworker.h"
#include "taoserror.h"
#include "tlog.h"

typedef void *(*ThreadFp)(void *param);

// 
int32_t tQWorkerInit(SQWorkerPool *pool) {
  pool->qset = taosOpenQset();// 初始化一个 qset
  pool->workers = taosMemoryCalloc(pool->max, sizeof(SQWorker));// 果然是个静态链表
  if (pool->workers == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  (void)taosThreadMutexInit(&pool->mutex, NULL);

  // 开始遍历 worker 进行初始化
  for (int32_t i = 0; i < pool->max; ++i) {
    SQWorker *worker = pool->workers + i;
    worker->id = i;
    worker->pool = pool;
  }

  uInfo("worker:%s is initialized, min:%d max:%d", pool->name, pool->min, pool->max);
  return 0;
}

// 清理 SQWorkerPool
void tQWorkerCleanup(SQWorkerPool *pool) {
  for (int32_t i = 0; i < pool->max; ++i) {
    SQWorker *worker = pool->workers + i;
    if (taosCheckPthreadValid(worker->thread)) {//判断是否为空，非空返回 true
      taosQsetThreadResume(pool->qset);//清除 qset (将内部信号量 + 1 以取消等待)
    }
  }

  // 等待线程结束并且清理线程
  for (int32_t i = 0; i < pool->max; ++i) {
    SQWorker *worker = pool->workers + i;
    if (taosCheckPthreadValid(worker->thread)) {
      uInfo("worker:%s:%d is stopping", pool->name, worker->id);
      taosThreadJoin(worker->thread, NULL);
      taosThreadClear(&worker->thread);
      uInfo("worker:%s:%d is stopped", pool->name, worker->id);
    }
  }

  taosMemoryFreeClear(pool->workers);
  taosCloseQset(pool->qset);
  taosThreadMutexDestroy(&pool->mutex);

  uInfo("worker:%s is closed", pool->name);
}

/*静态方法，worker 线程执行的线程实体*/
static void *tQWorkerThreadFp(SQWorker *worker) {
  SQWorkerPool *pool = worker->pool;
  SQueueInfo    qinfo = {0};
  void         *msg = NULL;
  int32_t       code = 0;

  taosBlockSIGPIPE();// linux 上初始化信号集
  setThreadName(pool->name);
  worker->pid = taosGetSelfPthreadId();//获取线程 pid
  uInfo("worker:%s:%d is running, thread:%08" PRId64, pool->name, worker->id, worker->pid);

  while (1) {
    //从 pool->qset 中取出 msg 和 qinfo，失败则跳出循环
    if (taosReadQitemFromQset(pool->qset, (void **)&msg, &qinfo) == 0) {
      uInfo("worker:%s:%d qset:%p, got no message and exiting, thread:%08" PRId64, pool->name, worker->id, pool->qset,
            worker->pid);
      break;
    }

    // 执行
    if (qinfo.fp != NULL) {
      qinfo.workerId = worker->id;
      qinfo.threadNum = pool->num;
      (*((FItem)qinfo.fp))(&qinfo, msg);
    }

    taosUpdateItemSize(qinfo.queue, 1);
  }

  return NULL;
}
//分配队列 ？？？
// @param 
// ahandle: 
// fp : 一个？？方法
STaosQueue *tQWorkerAllocQueue(SQWorkerPool *pool, void *ahandle, FItem fp) {
  STaosQueue *queue = taosOpenQueue();//新建一个 queue 对象,内含有一个 node
  if (queue == NULL) return NULL;

  taosThreadMutexLock(&pool->mutex);//锁定  SQWorkerPool
  taosSetQueueFp(queue, fp, NULL);//设置该队列的各项参数
  taosAddIntoQset(pool->qset, queue, ahandle);// ahandle 为 queue 中的成员，这里是将单个的元素 qset 放入到 queue 中

  // spawn a thread to process queue， 将线程派生到进程队列
  if (pool->num < pool->max) {
    do {
      SQWorker *worker = pool->workers + pool->num; // 定位执行任务的目标线程

      //初始化线程
      TdThreadAttr thAttr;
      taosThreadAttrInit(&thAttr);
      taosThreadAttrSetDetachState(&thAttr, PTHREAD_CREATE_JOINABLE);//设置线程可 join 

      // 初始化 worker 线程任务 ,创建失败则退出循环,第二次即退出
      if (taosThreadCreate(&worker->thread, &thAttr, (ThreadFp)tQWorkerThreadFp, worker) != 0) {
        taosCloseQueue(queue);//将 queue 从 qset 中脱离出来
        terrno = TSDB_CODE_OUT_OF_MEMORY;
        queue = NULL;
        break;
      }

      // 创建成功则摧毁线程 TdThreadAttr 
      taosThreadAttrDestroy(&thAttr);
      pool->num++;//递增
      uInfo("worker:%s:%d is launched, total:%d", pool->name, worker->id, pool->num);
    } while (pool->num < pool->min);
  }

  taosThreadMutexUnlock(&pool->mutex);
  uInfo("worker:%s, queue:%p is allocated, ahandle:%p", pool->name, queue, ahandle);

  return queue;
}
// 释放队列
void tQWorkerFreeQueue(SQWorkerPool *pool, STaosQueue *queue) {
  uInfo("worker:%s, queue:%p is freed", pool->name, queue);
  taosCloseQueue(queue);
}

int32_t tAutoQWorkerInit(SAutoQWorkerPool *pool) {
  pool->qset = taosOpenQset();
  //此处调用 taosArrayInit 分配 2 个，实际上该 SArray 类型最小也存有 8 个
  pool->workers = taosArrayInit(2, sizeof(SQWorker *));
  if (pool->workers == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  (void)taosThreadMutexInit(&pool->mutex, NULL);

  uInfo("worker:%s is initialized as auto", pool->name);
  return 0;
}

void tAutoQWorkerCleanup(SAutoQWorkerPool *pool) {
  int32_t size = taosArrayGetSize(pool->workers);
  for (int32_t i = 0; i < size; ++i) {
    SQWorker *worker = taosArrayGetP(pool->workers, i);
    if (taosCheckPthreadValid(worker->thread)) {
      taosQsetThreadResume(pool->qset);
    }
  }

  for (int32_t i = 0; i < size; ++i) {
    SQWorker *worker = taosArrayGetP(pool->workers, i);
    if (taosCheckPthreadValid(worker->thread)) {
      uInfo("worker:%s:%d is stopping", pool->name, worker->id);
      taosThreadJoin(worker->thread, NULL);
      taosThreadClear(&worker->thread);
      uInfo("worker:%s:%d is stopped", pool->name, worker->id);
    }
    taosMemoryFree(worker);
  }

  taosArrayDestroy(pool->workers);
  taosCloseQset(pool->qset);
  taosThreadMutexDestroy(&pool->mutex);

  uInfo("worker:%s is closed", pool->name);
}
/**/
static void *tAutoQWorkerThreadFp(SQWorker *worker) {
  SAutoQWorkerPool *pool = worker->pool;
  SQueueInfo        qinfo = {0};
  void             *msg = NULL;
  int32_t           code = 0;

  taosBlockSIGPIPE();
  setThreadName(pool->name);
  worker->pid = taosGetSelfPthreadId();
  uInfo("worker:%s:%d is running, thread:%08" PRId64, pool->name, worker->id, worker->pid);

  while (1) {
    if (taosReadQitemFromQset(pool->qset, (void **)&msg, &qinfo) == 0) {
      uInfo("worker:%s:%d qset:%p, got no message and exiting, thread:%08" PRId64, pool->name, worker->id, pool->qset,
            worker->pid);
      break;
    }

    if (qinfo.fp != NULL) {
      qinfo.workerId = worker->id;
      qinfo.threadNum = taosArrayGetSize(pool->workers);
      (*((FItem)qinfo.fp))(&qinfo, msg);
    }

    taosUpdateItemSize(qinfo.queue, 1);//减去一个值
  }

  return NULL;
}

STaosQueue *tAutoQWorkerAllocQueue(SAutoQWorkerPool *pool, void *ahandle, FItem fp) {
  STaosQueue *queue = taosOpenQueue();
  if (queue == NULL) return NULL;

  taosThreadMutexLock(&pool->mutex);
  taosSetQueueFp(queue, fp, NULL);
  taosAddIntoQset(pool->qset, queue, ahandle);

  int32_t queueNum = taosGetQueueNumber(pool->qset);
  int32_t curWorkerNum = taosArrayGetSize(pool->workers);
  int32_t dstWorkerNum = ceil(queueNum * pool->ratio);
  if (dstWorkerNum < 1) dstWorkerNum = 1;

  // spawn a thread to process queue
  while (curWorkerNum < dstWorkerNum) {
    SQWorker *worker = taosMemoryCalloc(1, sizeof(SQWorker));
    if (worker == NULL || taosArrayPush(pool->workers, &worker) == NULL) {
      uError("worker:%s:%d failed to create", pool->name, curWorkerNum);
      taosMemoryFree(worker);
      taosCloseQueue(queue);
      taosThreadMutexUnlock(&pool->mutex);
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      return NULL;
    }
    worker->id = curWorkerNum;
    worker->pool = pool;

    TdThreadAttr thAttr;
    taosThreadAttrInit(&thAttr);
    taosThreadAttrSetDetachState(&thAttr, PTHREAD_CREATE_JOINABLE);

    if (taosThreadCreate(&worker->thread, &thAttr, (ThreadFp)tAutoQWorkerThreadFp, worker) != 0) {
      uError("worker:%s:%d failed to create thread, total:%d", pool->name, worker->id, curWorkerNum);
      (void)taosArrayPop(pool->workers);
      taosMemoryFree(worker);
      taosCloseQueue(queue);
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      return NULL;
    }

    taosThreadAttrDestroy(&thAttr);
    uInfo("worker:%s:%d is launched, total:%d", pool->name, worker->id, (int32_t)taosArrayGetSize(pool->workers));

    curWorkerNum++;
  }

  taosThreadMutexUnlock(&pool->mutex);
  uInfo("worker:%s, queue:%p is allocated, ahandle:%p", pool->name, queue, ahandle);

  return queue;
}

void tAutoQWorkerFreeQueue(SAutoQWorkerPool *pool, STaosQueue *queue) {
  uInfo("worker:%s, queue:%p is freed", pool->name, queue);
  taosCloseQueue(queue);
}

int32_t tWWorkerInit(SWWorkerPool *pool) {
  pool->nextId = 0;
  pool->workers = taosMemoryCalloc(pool->max, sizeof(SWWorker));
  if (pool->workers == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  (void)taosThreadMutexInit(&pool->mutex, NULL);

  for (int32_t i = 0; i < pool->max; ++i) {
    SWWorker *worker = pool->workers + i;
    worker->id = i;
    worker->qall = NULL;
    worker->qset = NULL;
    worker->pool = pool;
  }

  uInfo("worker:%s is initialized, max:%d", pool->name, pool->max);
  return 0;
}

void tWWorkerCleanup(SWWorkerPool *pool) {
  for (int32_t i = 0; i < pool->max; ++i) {
    SWWorker *worker = pool->workers + i;
    if (taosCheckPthreadValid(worker->thread)) {
      if (worker->qset) {
        taosQsetThreadResume(worker->qset);
      }
    }
  }

  for (int32_t i = 0; i < pool->max; ++i) {
    SWWorker *worker = pool->workers + i;
    if (taosCheckPthreadValid(worker->thread)) {
      uInfo("worker:%s:%d is stopping", pool->name, worker->id);
      taosThreadJoin(worker->thread, NULL);
      taosThreadClear(&worker->thread);
      taosFreeQall(worker->qall);
      taosCloseQset(worker->qset);
      uInfo("worker:%s:%d is stopped", pool->name, worker->id);
    }
  }

  taosMemoryFreeClear(pool->workers);
  taosThreadMutexDestroy(&pool->mutex);

  uInfo("worker:%s is closed", pool->name);
}

/* 该类线程应该是一旦创建就会一直运行，因为 taosReadAllQitemsFromQset 中有 tsem_wait操作，
* 如果没有任务，那么将会等待，如果有任务将会取出并且处理。
*/
static void *tWWorkerThreadFp(SWWorker *worker) {
  SWWorkerPool *pool = worker->pool;
  SQueueInfo    qinfo = {0};
  void         *msg = NULL;
  int32_t       code = 0;
  int32_t       numOfMsgs = 0;

  taosBlockSIGPIPE();
  setThreadName(pool->name);
  worker->pid = taosGetSelfPthreadId();
  uInfo("worker:%s:%d is running, thread:%08" PRId64, pool->name, worker->id, worker->pid);

  while (1) {
    // 从 worker 维护的 qset 中取出一个任务队列到 qall 
    numOfMsgs = taosReadAllQitemsFromQset(worker->qset, worker->qall, &qinfo);
    if (numOfMsgs == 0) {
      uInfo("worker:%s:%d qset:%p, got no message and exiting, thread:%08" PRId64, pool->name, worker->id, worker->qset,
            worker->pid);
      break;
    }

    if (qinfo.fp != NULL) {
      qinfo.workerId = worker->id;
      qinfo.threadNum = pool->num;
      (*((FItems)qinfo.fp))(&qinfo, worker->qall, numOfMsgs);//vmProcessSyncQueue
    }
    taosUpdateItemSize(qinfo.queue, numOfMsgs);
  }

  return NULL;
}

STaosQueue *tWWorkerAllocQueue(SWWorkerPool *pool, void *ahandle, FItems fp) {
  taosThreadMutexLock(&pool->mutex);
  //定位 worker，
  SWWorker *worker = pool->workers + pool->nextId;
  int32_t   code = -1;

  STaosQueue *queue = taosOpenQueue();
  if (queue == NULL) goto _OVER;

  taosSetQueueFp(queue, NULL, fp);
  if (worker->qset == NULL) {
    worker->qset = taosOpenQset();
    if (worker->qset == NULL) goto _OVER;

    taosAddIntoQset(worker->qset, queue, ahandle);
    worker->qall = taosAllocateQall();
    if (worker->qall == NULL) goto _OVER;

    TdThreadAttr thAttr;
    taosThreadAttrInit(&thAttr);
    taosThreadAttrSetDetachState(&thAttr, PTHREAD_CREATE_JOINABLE);
    if (taosThreadCreate(&worker->thread, &thAttr, (ThreadFp)tWWorkerThreadFp, worker) != 0) goto _OVER;

    uInfo("worker:%s:%d is launched, max:%d", pool->name, worker->id, pool->max);
    pool->nextId = (pool->nextId + 1) % pool->max;

    taosThreadAttrDestroy(&thAttr);
    pool->num++;
    if (pool->num > pool->max) pool->num = pool->max;
  } else {
    taosAddIntoQset(worker->qset, queue, ahandle);
    pool->nextId = (pool->nextId + 1) % pool->max;
  }

  code = 0;

_OVER:
  taosThreadMutexUnlock(&pool->mutex);

  if (code == -1) {
    if (queue != NULL) taosCloseQueue(queue);
    if (worker->qset != NULL) taosCloseQset(worker->qset);
    if (worker->qall != NULL) taosFreeQall(worker->qall);
    return NULL;
  } else {
    while (worker->pid <= 0) taosMsleep(10);
    queue->threadId = worker->pid;
    uInfo("worker:%s, queue:%p is allocated, ahandle:%p thread:%08" PRId64, pool->name, queue, ahandle,
          queue->threadId);
    return queue;
  }
}

void tWWorkerFreeQueue(SWWorkerPool *pool, STaosQueue *queue) {
  uInfo("worker:%s, queue:%p is freed", pool->name, queue);
  taosCloseQueue(queue);
}

int32_t tSingleWorkerInit(SSingleWorker *pWorker, const SSingleWorkerCfg *pCfg) {
  SQWorkerPool *pPool = &pWorker->pool;
  pPool->name = pCfg->name;
  pPool->min = pCfg->min;
  pPool->max = pCfg->max;
  if (tQWorkerInit(pPool) != 0) return -1;

  pWorker->queue = tQWorkerAllocQueue(pPool, pCfg->param, pCfg->fp);
  if (pWorker->queue == NULL) return -1;

  pWorker->name = pCfg->name;
  return 0;
}

void tSingleWorkerCleanup(SSingleWorker *pWorker) {
  if (pWorker->queue == NULL) return;

  while (!taosQueueEmpty(pWorker->queue)) {
    taosMsleep(10);
  }

  tQWorkerCleanup(&pWorker->pool);
  tQWorkerFreeQueue(&pWorker->pool, pWorker->queue);
}

int32_t tMultiWorkerInit(SMultiWorker *pWorker, const SMultiWorkerCfg *pCfg) {
  SWWorkerPool *pPool = &pWorker->pool;
  pPool->name = pCfg->name;
  pPool->max = pCfg->max;
  if (tWWorkerInit(pPool) != 0) return -1;

  pWorker->queue = tWWorkerAllocQueue(pPool, pCfg->param, pCfg->fp);
  if (pWorker->queue == NULL) return -1;

  pWorker->name = pCfg->name;
  return 0;
}

void tMultiWorkerCleanup(SMultiWorker *pWorker) {
  if (pWorker->queue == NULL) return;

  while (!taosQueueEmpty(pWorker->queue)) {
    taosMsleep(10);
  }

  tWWorkerCleanup(&pWorker->pool);
  tWWorkerFreeQueue(&pWorker->pool, pWorker->queue);
}
