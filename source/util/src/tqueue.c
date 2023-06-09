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
#include "tqueue.h"
#include "taoserror.h"
#include "tlog.h"

int64_t tsRpcQueueMemoryAllowed = 0;
int64_t tsRpcQueueMemoryUsed = 0;

STaosQueue *taosOpenQueue() {
  STaosQueue *queue = taosMemoryCalloc(1, sizeof(STaosQueue));
  if (queue == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }

  if (taosThreadMutexInit(&queue->mutex, NULL) != 0) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }

  uDebug("queue:%p is opened", queue);
  return queue;
}

void taosSetQueueFp(STaosQueue *queue, FItem itemFp, FItems itemsFp) {
  if (queue == NULL) return;
  queue->itemFp = itemFp;
  queue->itemsFp = itemsFp;
}

void taosCloseQueue(STaosQueue *queue) {
  if (queue == NULL) return;
  STaosQnode *pTemp;
  STaosQset  *qset;

  taosThreadMutexLock(&queue->mutex);
  STaosQnode *pNode = queue->head;
  queue->head = NULL;
  qset = queue->qset;
  taosThreadMutexUnlock(&queue->mutex);

  if (queue->qset) {
    taosRemoveFromQset(qset, queue);
  }

  while (pNode) {
    pTemp = pNode;
    pNode = pNode->next;
    taosMemoryFree(pTemp);
  }

  taosThreadMutexDestroy(&queue->mutex);
  taosMemoryFree(queue);

  uDebug("queue:%p is closed", queue);
}

bool taosQueueEmpty(STaosQueue *queue) {
  if (queue == NULL) return true;

  bool empty = false;
  taosThreadMutexLock(&queue->mutex);
  if (queue->head == NULL && queue->tail == NULL && queue->numOfItems == 0 && queue->memOfItems == 0) {
    empty = true;
  }
  taosThreadMutexUnlock(&queue->mutex);

  return empty;
}

void taosUpdateItemSize(STaosQueue *queue, int32_t items) {
  if (queue == NULL) return;

  taosThreadMutexLock(&queue->mutex);
  queue->numOfItems -= items;
  taosThreadMutexUnlock(&queue->mutex);
}

int32_t taosQueueItemSize(STaosQueue *queue) {
  if (queue == NULL) return 0;

  taosThreadMutexLock(&queue->mutex);
  int32_t numOfItems = queue->numOfItems;
  taosThreadMutexUnlock(&queue->mutex);

  uTrace("queue:%p, numOfItems:%d memOfItems:%" PRId64, queue, queue->numOfItems, queue->memOfItems);
  return numOfItems;
}

int64_t taosQueueMemorySize(STaosQueue *queue) {
  taosThreadMutexLock(&queue->mutex);
  int64_t memOfItems = queue->memOfItems;
  taosThreadMutexUnlock(&queue->mutex);
  return memOfItems;
}
/* 分配 size 量的 STaosQnode ，其内部可存放 dataSize 空间内容
* 返回的是 该 STaosQnode 的 item ，这个 item 一般就是 SRpcMsg 。
* 在 mnode 中， 存的方法是通过 SRpcMsg 的 msgType 进行匹配定位，应该是一个 token 。
*/
void *taosAllocateQitem(int32_t size, EQItype itype, int64_t dataSize) {
  STaosQnode *pNode = taosMemoryCalloc(1, sizeof(STaosQnode) + size);
  if (pNode == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }

  pNode->dataSize = dataSize;
  pNode->size = size;
  pNode->itype = itype;
  pNode->timestamp = taosGetTimestampUs();

  if (itype == RPC_QITEM) {
    int64_t alloced = atomic_add_fetch_64(&tsRpcQueueMemoryUsed, size + dataSize);
    if (alloced > tsRpcQueueMemoryAllowed) {
      uError("failed to alloc qitem, size:%" PRId64 " alloc:%" PRId64 " allowed:%" PRId64, size + dataSize, alloced,
             tsRpcQueueMemoryUsed);
      atomic_sub_fetch_64(&tsRpcQueueMemoryUsed, size + dataSize);
      taosMemoryFree(pNode);
      terrno = TSDB_CODE_OUT_OF_RPC_MEMORY_QUEUE;
      return NULL;
    }
    uTrace("item:%p, node:%p is allocated, alloc:%" PRId64, pNode->item, pNode, alloced);
  } else {
    uTrace("item:%p, node:%p is allocated", pNode->item, pNode);
  }

  return pNode->item;
}

// tsRpcQueueMemoryUsed 减去 pItem 所据有的实际数据空间，并且释放 pItem 关联的 pNode 。
void taosFreeQitem(void *pItem) {
  if (pItem == NULL) return;

  STaosQnode *pNode = (STaosQnode *)((char *)pItem - sizeof(STaosQnode));
  if (pNode->itype == RPC_QITEM) {
    int64_t alloced = atomic_sub_fetch_64(&tsRpcQueueMemoryUsed, pNode->size + pNode->dataSize);
    uTrace("item:%p, node:%p is freed, alloc:%" PRId64, pItem, pNode, alloced);
  } else {
    uTrace("item:%p, node:%p is freed", pItem, pNode);
  }

  taosMemoryFree(pNode);
}

/* 写入到指定 queue 中， 并且通知读取方进行读取
* 该部分中， pItem 通过指针减运算， 复原出 pItem 地址前的 STaosQnode 
* 但是这部分到底存的是什么还有待商榷
*/
void taosWriteQitem(STaosQueue *queue, void *pItem) {
  STaosQnode *pNode = (STaosQnode *)(((char *)pItem) - sizeof(STaosQnode));
  pNode->next = NULL;

  taosThreadMutexLock(&queue->mutex);

  if (queue->tail) {
    queue->tail->next = pNode;
    queue->tail = pNode;
  } else {
    queue->head = pNode;
    queue->tail = pNode;
  }

  queue->numOfItems++;
  queue->memOfItems += pNode->size;
  if (queue->qset) atomic_add_fetch_32(&queue->qset->numOfItems, 1);
  uTrace("item:%p is put into queue:%p, items:%d mem:%" PRId64, pItem, queue, queue->numOfItems, queue->memOfItems);

  taosThreadMutexUnlock(&queue->mutex);

  if (queue->qset) tsem_post(&queue->qset->sem);
}

int32_t taosReadQitem(STaosQueue *queue, void **ppItem) {
  STaosQnode *pNode = NULL;
  int32_t     code = 0;

  taosThreadMutexLock(&queue->mutex);

  if (queue->head) {
    pNode = queue->head;
    *ppItem = pNode->item;
    queue->head = pNode->next;
    if (queue->head == NULL) queue->tail = NULL;
    queue->numOfItems--;
    queue->memOfItems -= pNode->size;
    if (queue->qset) atomic_sub_fetch_32(&queue->qset->numOfItems, 1);
    code = 1;
    uTrace("item:%p is read out from queue:%p, items:%d mem:%" PRId64, *ppItem, queue, queue->numOfItems,
           queue->memOfItems);
  }

  taosThreadMutexUnlock(&queue->mutex);

  return code;
}

STaosQall *taosAllocateQall() {
  STaosQall *qall = taosMemoryCalloc(1, sizeof(STaosQall));
  if (qall != NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
  }
  return qall;
}

void taosFreeQall(STaosQall *qall) { taosMemoryFree(qall); }
/* 清空 queue 和 qall 并且将所有内容存放到 qall，并且修改 queue 的关联上级*/
int32_t taosReadAllQitems(STaosQueue *queue, STaosQall *qall) {
  int32_t numOfItems = 0;
  bool    empty;

  taosThreadMutexLock(&queue->mutex);

  empty = queue->head == NULL;
  if (!empty) {
    memset(qall, 0, sizeof(STaosQall));
    qall->current = queue->head;
    qall->start = queue->head;
    qall->numOfItems = queue->numOfItems;
    numOfItems = qall->numOfItems;

    queue->head = NULL;
    queue->tail = NULL;
    queue->numOfItems = 0;
    queue->memOfItems = 0;
    uTrace("read %d items from queue:%p, items:%d mem:%" PRId64, numOfItems, queue, queue->numOfItems,
           queue->memOfItems);
    if (queue->qset) atomic_sub_fetch_32(&queue->qset->numOfItems, qall->numOfItems);
  }

  taosThreadMutexUnlock(&queue->mutex);

  // if source queue is empty, we set destination qall to empty too.
  if (empty) {
    qall->current = NULL;
    qall->start = NULL;
    qall->numOfItems = 0;
  }
  return numOfItems;
}

int32_t taosGetQitem(STaosQall *qall, void **ppItem) {
  STaosQnode *pNode;
  int32_t     num = 0;

  pNode = qall->current;
  if (pNode) qall->current = pNode->next;

  if (pNode) {
    *ppItem = pNode->item;
    num = 1;
    uTrace("item:%p is fetched", *ppItem);
  } else {
    *ppItem = NULL;
  }

  return num;
}

STaosQset *taosOpenQset() {
  STaosQset *qset = taosMemoryCalloc(sizeof(STaosQset), 1);
  if (qset == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }

  taosThreadMutexInit(&qset->mutex, NULL);
  tsem_init(&qset->sem, 0, 0);

  uDebug("qset:%p is opened", qset);
  return qset;
}

void taosCloseQset(STaosQset *qset) {
  if (qset == NULL) return;

  // remove all the queues from qset
  taosThreadMutexLock(&qset->mutex);
  while (qset->head) {
    STaosQueue *queue = qset->head;
    qset->head = qset->head->next;

    queue->qset = NULL;
    queue->next = NULL;
  }
  taosThreadMutexUnlock(&qset->mutex);

  taosThreadMutexDestroy(&qset->mutex);
  tsem_destroy(&qset->sem);
  taosMemoryFree(qset);
  uDebug("qset:%p is closed", qset);
}

// tsem_post 'qset->sem', so that reader threads waiting for it
// resumes execution and return, should only be used to signal the
// thread to exit.
void taosQsetThreadResume(STaosQset *qset) {
  uDebug("qset:%p, it will exit", qset);
  tsem_post(&qset->sem);
}

int32_t taosAddIntoQset(STaosQset *qset, STaosQueue *queue, void *ahandle) {
  if (queue->qset) return -1;

  taosThreadMutexLock(&qset->mutex);

  queue->next = qset->head;
  queue->ahandle = ahandle;
  qset->head = queue;
  qset->numOfQueues++;

  taosThreadMutexLock(&queue->mutex);
  atomic_add_fetch_32(&qset->numOfItems, queue->numOfItems);
  queue->qset = qset;
  taosThreadMutexUnlock(&queue->mutex);

  taosThreadMutexUnlock(&qset->mutex);

  uTrace("queue:%p is added into qset:%p", queue, qset);
  return 0;
}

void taosRemoveFromQset(STaosQset *qset, STaosQueue *queue) {
  STaosQueue *tqueue = NULL;

  taosThreadMutexLock(&qset->mutex);

  if (qset->head) {
    if (qset->head == queue) {
      qset->head = qset->head->next;
      tqueue = queue;
    } else {
      STaosQueue *prev = qset->head;
      tqueue = qset->head->next;
      while (tqueue) {
        assert(tqueue->qset);
        if (tqueue == queue) {
          prev->next = tqueue->next;
          break;
        } else {
          prev = tqueue;
          tqueue = tqueue->next;
        }
      }
    }

    if (tqueue) {
      if (qset->current == queue) qset->current = tqueue->next;
      qset->numOfQueues--;

      taosThreadMutexLock(&queue->mutex);
      atomic_sub_fetch_32(&qset->numOfItems, queue->numOfItems);
      queue->qset = NULL;
      queue->next = NULL;
      taosThreadMutexUnlock(&queue->mutex);
    }
  }

  taosThreadMutexUnlock(&qset->mutex);

  uDebug("queue:%p is removed from qset:%p", queue, qset);
}

// 返回 1 为成功， 0 为失败
// 从 qset 中随便读取一个值，一般是头队列
int32_t taosReadQitemFromQset(STaosQset *qset, void **ppItem, SQueueInfo *qinfo) {
  STaosQnode *pNode = NULL;
  int32_t     code = 0;

  tsem_wait(&qset->sem);

  taosThreadMutexLock(&qset->mutex);

  for (int32_t i = 0; i < qset->numOfQueues; ++i) {
    if (qset->current == NULL) qset->current = qset->head;
    STaosQueue *queue = qset->current;
    if (queue) qset->current = queue->next;
    if (queue == NULL) break;
    if (queue->head == NULL) continue;

    taosThreadMutexLock(&queue->mutex);

    if (queue->head) {
      pNode = queue->head;
      *ppItem = pNode->item;
      qinfo->ahandle = queue->ahandle;
      qinfo->fp = queue->itemFp;
      qinfo->queue = queue;
      qinfo->timestamp = pNode->timestamp;

      queue->head = pNode->next;
      if (queue->head == NULL) queue->tail = NULL;
      // queue->numOfItems--;
      queue->memOfItems -= pNode->size;
      atomic_sub_fetch_32(&qset->numOfItems, 1);
      code = 1;
      uTrace("item:%p is read out from queue:%p, items:%d mem:%" PRId64, *ppItem, queue, queue->numOfItems - 1,
             queue->memOfItems);
    }

    taosThreadMutexUnlock(&queue->mutex);
    if (pNode) break;
  }

  taosThreadMutexUnlock(&qset->mutex);

  return code;
}

int32_t taosReadAllQitemsFromQset(STaosQset *qset, STaosQall *qall, SQueueInfo *qinfo) {
  STaosQueue *queue;
  int32_t     code = 0;

  tsem_wait(&qset->sem);//判断/等待 qset 是否有数据
  taosThreadMutexLock(&qset->mutex);//锁定 qset

  for (int32_t i = 0; i < qset->numOfQueues; ++i) {//遍历整个 qset 的每一个 queue
    if (qset->current == NULL) qset->current = qset->head;//调整当前的 queue 指针
    queue = qset->current;//定位 queue
    if (queue) qset->current = queue->next;//将当前指针后移动一个
    if (queue == NULL) break;
    if (queue->head == NULL) continue;

    taosThreadMutexLock(&queue->mutex);//锁定 queue

    if (queue->head) {//如果当前的 queue 有值
      qall->current = queue->head;
      qall->start = queue->head;
      qall->numOfItems = queue->numOfItems;
      code = qall->numOfItems;
      qinfo->ahandle = queue->ahandle;//记录/复写信息
      qinfo->fp = queue->itemsFp;
      qinfo->queue = queue;

      queue->head = NULL;//重置载体
      queue->tail = NULL;
      // queue->numOfItems = 0;
      queue->memOfItems = 0;
      uTrace("read %d items from queue:%p, items:0 mem:%" PRId64, code, queue, queue->memOfItems);

      atomic_sub_fetch_32(&qset->numOfItems, qall->numOfItems);//原子操作，前者减去后者存入前者
      for (int32_t j = 1; j < qall->numOfItems; ++j) {
        tsem_wait(&qset->sem);//释放 qall 接管的数据量
      }
    }

    taosThreadMutexUnlock(&queue->mutex);

    if (code != 0) break;//只读一个系列即可
  }

  taosThreadMutexUnlock(&qset->mutex);
  return code;
}

int32_t taosQallItemSize(STaosQall *qall) { return qall->numOfItems; }
void    taosResetQitems(STaosQall *qall) { qall->current = qall->start; }
int32_t taosGetQueueNumber(STaosQset *qset) { return qset->numOfQueues; }

#if 0

void taosResetQsetThread(STaosQset *qset, void *pItem) {
  if (pItem == NULL) return;
  STaosQnode *pNode = (STaosQnode *)((char *)pItem - sizeof(STaosQnode));

  taosThreadMutexLock(&qset->mutex);
  for (int32_t i = 0; i < pNode->queue->numOfItems; ++i) {
    tsem_post(&qset->sem);
  }
  taosThreadMutexUnlock(&qset->mutex);
}

#endif
