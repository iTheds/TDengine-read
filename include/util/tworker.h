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

#ifndef _TD_UTIL_WORKER_H_
#define _TD_UTIL_WORKER_H_

#include "tqueue.h"
#include "tarray.h"

#ifdef __cplusplus
extern "C" {
#endif

/*
工作者模式：
*/

typedef struct SQWorkerPool SQWorkerPool;
typedef struct SWWorkerPool SWWorkerPool;

// 最基本的 worker 
typedef struct SQWorker {
  int32_t  id;      // worker id - 随便起的非冲突字段
  int64_t  pid;     // thread pid
  TdThread thread;  // thread id
  void    *pool;    // 指向统一的上层 pool 的指针,这里没有指定类型，应该是可以有多种选择
} SQWorker;
/*采用的是 SQWorker*/
typedef struct SQWorkerPool {
  int32_t       max;  // max number of workers
  int32_t       min;  // min number of workers
  int32_t       num;  // current number of workers
  STaosQset    *qset; // 任务队列？？
  const char   *name;
  SQWorker     *workers; // 工作者线程静态列表
  TdThreadMutex mutex;  //一个锁,该锁是为了保护 SQWorkerPool 本身
} SQWorkerPool;
/*采用的是 SArray 作为 worker*/
typedef struct SAutoQWorkerPool {
  float         ratio;
  STaosQset    *qset;
  const char   *name;
  SArray       *workers;
  TdThreadMutex mutex;
} SAutoQWorkerPool;
/*含有 STaosQall 和 STaosQset 的 worker ，专门为 SWWorkerPool 定制*/
typedef struct SWWorker {
  int32_t       id;      // worker id
  int64_t       pid;     // thread pid
  TdThread      thread;  // thread id
  STaosQall    *qall;
  STaosQset    *qset;
  SWWorkerPool *pool;
} SWWorker;
/*采用的是 SWWorker 作为 worker*/
typedef struct SWWorkerPool {
  int32_t       max;  // max number of workers
  int32_t       num;
  int32_t       nextId;  // from 0 to max-1, cyclic
  const char   *name;
  SWWorker     *workers;
  TdThreadMutex mutex;
} SWWorkerPool;

int32_t     tQWorkerInit(SQWorkerPool *pool);
void        tQWorkerCleanup(SQWorkerPool *pool);
STaosQueue *tQWorkerAllocQueue(SQWorkerPool *pool, void *ahandle, FItem fp);
void        tQWorkerFreeQueue(SQWorkerPool *pool, STaosQueue *queue);

int32_t     tAutoQWorkerInit(SAutoQWorkerPool *pool);
void        tAutoQWorkerCleanup(SAutoQWorkerPool *pool);
STaosQueue *tAutoQWorkerAllocQueue(SAutoQWorkerPool *pool, void *ahandle, FItem fp);
void        tAutoQWorkerFreeQueue(SAutoQWorkerPool *pool, STaosQueue *queue);

int32_t     tWWorkerInit(SWWorkerPool *pool);
void        tWWorkerCleanup(SWWorkerPool *pool);
STaosQueue *tWWorkerAllocQueue(SWWorkerPool *pool, void *ahandle, FItems fp);
void        tWWorkerFreeQueue(SWWorkerPool *pool, STaosQueue *queue);

typedef struct {
  const char *name;
  int32_t     min;
  int32_t     max;
  FItem       fp;
  void       *param;
} SSingleWorkerCfg;

typedef struct {
  const char  *name;
  STaosQueue  *queue;
  SQWorkerPool pool;
} SSingleWorker;

typedef struct {
  const char *name;
  int32_t     max;
  FItems      fp;
  void       *param;
} SMultiWorkerCfg;

typedef struct {
  const char  *name;
  STaosQueue  *queue;
  SWWorkerPool pool;
} SMultiWorker;

int32_t tSingleWorkerInit(SSingleWorker *pWorker, const SSingleWorkerCfg *pCfg);
void    tSingleWorkerCleanup(SSingleWorker *pWorker);
int32_t tMultiWorkerInit(SMultiWorker *pWorker, const SMultiWorkerCfg *pCfg);
void    tMultiWorkerCleanup(SMultiWorker *pWorker);

#ifdef __cplusplus
}
#endif

#endif /*_TD_UTIL_WORKER_H_*/
