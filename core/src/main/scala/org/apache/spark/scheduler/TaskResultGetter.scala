/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.scheduler

import java.nio.ByteBuffer
import java.util.concurrent.{ExecutorService, RejectedExecutionException}

import scala.language.existentials
import scala.util.control.NonFatal

import org.apache.spark._
import org.apache.spark.TaskState.TaskState
import org.apache.spark.internal.Logging
import org.apache.spark.serializer.SerializerInstance
import org.apache.spark.util.{LongAccumulator, ThreadUtils, Utils}

/**
 * Runs a thread pool that deserializes and remotely fetches (if necessary) task results.
 */
private[spark] class TaskResultGetter(sparkEnv: SparkEnv, scheduler: TaskSchedulerImpl)
  extends Logging {
  // 获取Task执行结果的线程数,默认为4
  private val THREADS = sparkEnv.conf.getInt("spark.resultGetter.threads", 4)

  // 用于提交获取Task执行结果的线程。线程池的大小由THREADS决定
  protected val getTaskResultExecutor: ExecutorService =
    ThreadUtils.newDaemonFixedThreadPool(THREADS, "task-result-getter")

  // Exposed for testing.
  protected val serializer = new ThreadLocal[SerializerInstance] {
    override def initialValue(): SerializerInstance = {
      sparkEnv.closureSerializer.newInstance()
    }
  }

  protected val taskResultSerializer = new ThreadLocal[SerializerInstance] {
    override def initialValue(): SerializerInstance = {
      sparkEnv.serializer.newInstance()
    }
  }

  def enqueueSuccessfulTask(
      taskSetManager: TaskSetManager,
      tid: Long,
      serializedData: ByteBuffer): Unit = {
    /*
       enqueueSuccessfulTask方法实际向getTaskResultExecutor提交了一个获取Task执行结果的任务，
       这个任务的执行步骤如下。
     */
    getTaskResultExecutor.execute(new Runnable {
      override def run(): Unit = Utils.logUncaughtExceptions {
        try {
          val (result, size) = serializer.get().deserialize[TaskResult[_]](serializedData) match {
            /*
               对Task的执行结果反序列化，如果Task的结果类型为DirectTaskResult，说明 Task的执行结果保存
               在DirectTaskResult中，此时只需要对DirectTaskResult保存的数据 (即DirectTaskResult的valueBytes属性）
               进行反序列化就可以得到。

               DirectTaskResult----> A TaskResult that contains the task's return value and accumulator updates
             */
            case directResult: DirectTaskResult[_] =>
              //  判断总的结果是否大于限制大小
              if (!taskSetManager.canFetchMoreResults(serializedData.limit())) {
                // kill the task so that it will not become zombie task
                scheduler.handleFailedTask(taskSetManager, tid, TaskState.KILLED, TaskKilled(
                  "Tasks result size has exceeded maxResultSize"))
                return
              }
              // deserialize "value" without holding any lock so that it won't block other threads.
              // We should call it here, so that when it's called again in
              // "TaskSetManager.handleSuccessfulTask", it does not need to deserialize the value.
              directResult.value(taskResultSerializer.get())
              (directResult, serializedData.limit())
              /*
                如果Task的结果类型为IndirectTaskResult，说 明Task的执行结果没有保存在IndirectTaskResult中，
                此时需要调用TaskSchedulerlmpl 的handleTaskGettingResult方法（此方法的实现非常简单，这里就
                不过多介绍了)，向 DAGSchedulerEventProcessLoop投递GettingResultEvent 事件，然后调用
                BlockManager 的 getRemoteBytes方法（见代码清单6-75)，从运行Task的节点上下载Block,最后对
                下载到 的数据反序列化得到Task的执行结果。
               */
            case IndirectTaskResult(blockId, size) =>
              if (!taskSetManager.canFetchMoreResults(size)) {
                // dropped by executor if size is larger than maxResultSize
                sparkEnv.blockManager.master.removeBlock(blockId)
                // kill the task so that it will not become zombie task
                scheduler.handleFailedTask(taskSetManager, tid, TaskState.KILLED, TaskKilled(
                  "Tasks result size has exceeded maxResultSize"))
                return
              }
              logDebug("Fetching indirect task result for TID %s".format(tid))
              scheduler.handleTaskGettingResult(taskSetManager, tid)
              val serializedTaskResult = sparkEnv.blockManager.getRemoteBytes(blockId)
              if (!serializedTaskResult.isDefined) {
                /* We won't be able to get the task result if the machine that ran the task failed
                 * between when the task ended and when we tried to fetch the result, or if the
                 * block manager had to flush the result. */
                scheduler.handleFailedTask(
                  taskSetManager, tid, TaskState.FINISHED, TaskResultLost)
                return
              }
              val deserializedResult = serializer.get().deserialize[DirectTaskResult[_]](
                serializedTaskResult.get.toByteBuffer)
              // force deserialization of referenced value
              deserializedResult.value(taskResultSerializer.get())
              sparkEnv.blockManager.master.removeBlock(blockId)
              (deserializedResult, size)
          }

          // Set the task result size in the accumulator updates received from the executors.
          // We need to do this here on the driver because if we did this on the executors then
          // we would have to serialize the result again after updating the size.
          /*
              更新执行结果的累加器中的大小
           */
          result.accumUpdates = result.accumUpdates.map { a =>
            if (a.name == Some(InternalAccumulator.RESULT_SIZE)) {
              val acc = a.asInstanceOf[LongAccumulator]
              assert(acc.sum == 0L, "task result size should not have been set on the executors")
              acc.setValue(size.toLong)
              acc
            } else {
              a
            }
          }

          scheduler.handleSuccessfulTask(taskSetManager, tid, result)
        } catch {
          case cnf: ClassNotFoundException =>
            val loader = Thread.currentThread.getContextClassLoader
            taskSetManager.abort("ClassNotFound with classloader: " + loader)
          // Matching NonFatal so we don't catch the ControlThrowable from the "return" above.
          case NonFatal(ex) =>
            logError("Exception while getting task result", ex)
            taskSetManager.abort("Exception while getting task result: %s".format(ex))
        }
      }
    })
  }

  def enqueueFailedTask(taskSetManager: TaskSetManager, tid: Long, taskState: TaskState,
    serializedData: ByteBuffer) {
    var reason : TaskFailedReason = UnknownReason
    try {
      getTaskResultExecutor.execute(new Runnable {
        override def run(): Unit = Utils.logUncaughtExceptions {
          val loader = Utils.getContextOrSparkClassLoader
          try {
            if (serializedData != null && serializedData.limit() > 0) {
              reason = serializer.get().deserialize[TaskFailedReason](
                serializedData, loader)
            }
          } catch {
            case cnd: ClassNotFoundException =>
              // Log an error but keep going here -- the task failed, so not catastrophic
              // if we can't deserialize the reason.
              logError(
                "Could not deserialize TaskEndReason: ClassNotFound with classloader " + loader)
            case ex: Exception => // No-op
          } finally {
            // If there's an error while deserializing the TaskEndReason, this Runnable
            // will die. Still tell the scheduler about the task failure, to avoid a hang
            // where the scheduler thinks the task is still running.
            scheduler.handleFailedTask(taskSetManager, tid, taskState, reason)
          }
        }
      })
    } catch {
      case e: RejectedExecutionException if sparkEnv.isStopped =>
        // ignore it
    }
  }

  def stop() {
    getTaskResultExecutor.shutdownNow()
  }
}
