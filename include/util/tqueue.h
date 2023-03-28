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

#ifndef _TD_UTIL_QUEUE_H_
#define _TD_UTIL_QUEUE_H_

#include "os.h"

#ifdef __cplusplus
extern "C" {
#endif

/*

This set of API for queue is designed specially for vnode/mnode. The main purpose is to
consume all the items instead of one item from a queue by one single read. Also, it can
combine multiple queues into a queue set, a consumer thread can consume a queue set via
a single API instead of looping every queue by itself.

Notes:
1: taosOpenQueue/taosCloseQueue, taosOpenQset/taosCloseQset is NOT multi-thread safe
2: after taosCloseQueue/taosCloseQset is called, read/write operation APIs are not safe.
3: read/write operation APIs are multi-thread safe

To remove the limitation and make this set of queue APIs multi-thread safe, REF(tref.c)
shall be used to set up the protection.

*/

typedef struct STaosQueue STaosQueue;
typedef struct STaosQset  STaosQset;
typedef struct STaosQall  STaosQall;
// 基本的信息，作为方法函数的 入参
typedef struct {
  void   *ahandle; //？？
  void   *fp; // 存有的任务
  void   *queue;//可能还是上级指针
  int32_t workerId;
  int32_t threadNum;
  int64_t timestamp;
} SQueueInfo;

typedef enum {
  DEF_QITEM = 0,// def 
  RPC_QITEM = 1,// prc 
} EQItype;

typedef void (*FItem)(SQueueInfo *pInfo, void *pItem);
typedef void (*FItems)(SQueueInfo *pInfo, STaosQall *qall, int32_t numOfItems);

typedef struct STaosQnode STaosQnode;

// 单个节点，存有上级队列 Queue 和下个节点 Qnode 指针。
// 以 `char item[]` 作为数据存放点。
typedef struct STaosQnode {
  STaosQnode *next;
  STaosQueue *queue;
  int64_t     timestamp;
  int64_t     dataSize;
  int32_t     size;
  int8_t      itype;
  int8_t      reserved[3];
  char        item[];
} STaosQnode;

// 队列， 存储的是 node 
typedef struct STaosQueue {
  STaosQnode   *head;
  STaosQnode   *tail;
  STaosQueue   *next;     //
  STaosQset    *qset;     // 上级 Qset???
  void         *ahandle;  //
  FItem         itemFp;   // 一个函数指针类型
  FItems        itemsFp;
  TdThreadMutex mutex;    //该锁是用于保护该类自身的所有数据
  int64_t       memOfItems;
  int32_t       numOfItems;
  int64_t       threadId;
} STaosQueue;

// 一个存储了队列集合的类 qset,
// qset 拥有一个特定的锁和信号量，
// 存储了 队列<STaosQueue>(实际为集合理论概念) 的头节点和当前节点位置
typedef struct STaosQset {
  STaosQueue   *head;
  STaosQueue   *current;
  TdThreadMutex mutex;  //该锁是为了？？？
  tsem_t        sem;    //计数表示写入的次数，写入后将内容放入？？
  int32_t       numOfQueues;
  int32_t       numOfItems;
} STaosQset;

// 存放 STaosQnode 头和当前位置指针，
// 看起来和 STaosQueue 结构职能是冲突的，
// 实际上其作用？？？
typedef struct STaosQall {
  STaosQnode *current;
  STaosQnode *start;
  int32_t     numOfItems;
} STaosQall;

STaosQueue *taosOpenQueue();
void        taosCloseQueue(STaosQueue *queue);
void        taosSetQueueFp(STaosQueue *queue, FItem itemFp, FItems itemsFp);
// 新建一个 STaosQnode ，返回其 item
void       *taosAllocateQitem(int32_t size, EQItype itype, int64_t dataSize);
void        taosFreeQitem(void *pItem);
void        taosWriteQitem(STaosQueue *queue, void *pItem);
int32_t     taosReadQitem(STaosQueue *queue, void **ppItem);
bool        taosQueueEmpty(STaosQueue *queue);
void        taosUpdateItemSize(STaosQueue *queue, int32_t items);
int32_t     taosQueueItemSize(STaosQueue *queue);
int64_t     taosQueueMemorySize(STaosQueue *queue);

STaosQall *taosAllocateQall();
void       taosFreeQall(STaosQall *qall);
int32_t    taosReadAllQitems(STaosQueue *queue, STaosQall *qall);
int32_t    taosGetQitem(STaosQall *qall, void **ppItem);
void       taosResetQitems(STaosQall *qall);
int32_t    taosQallItemSize(STaosQall *qall);

STaosQset *taosOpenQset();
void       taosCloseQset(STaosQset *qset);
// qset 特有，将所有信号量置空
void       taosQsetThreadResume(STaosQset *qset);
int32_t    taosAddIntoQset(STaosQset *qset, STaosQueue *queue, void *ahandle);
void       taosRemoveFromQset(STaosQset *qset, STaosQueue *queue);
int32_t    taosGetQueueNumber(STaosQset *qset);

int32_t taosReadQitemFromQset(STaosQset *qset, void **ppItem, SQueueInfo *qinfo);
int32_t taosReadAllQitemsFromQset(STaosQset *qset, STaosQall *qall, SQueueInfo *qinfo);
void    taosResetQsetThread(STaosQset *qset, void *pItem);

extern int64_t tsRpcQueueMemoryAllowed;

#ifdef __cplusplus
}
#endif

#endif /*_TD_UTIL_QUEUE_H_*/
