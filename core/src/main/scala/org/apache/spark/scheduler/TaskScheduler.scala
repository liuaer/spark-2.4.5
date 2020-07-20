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

import org.apache.spark.scheduler.SchedulingMode.SchedulingMode
import org.apache.spark.storage.BlockManagerId
import org.apache.spark.util.AccumulatorV2

/**
  * 任务调度器TaskScheduler定义了对任务进行调度的接口规范，允许向Spark调度系统插人不同的TaskScheduler实现，
  * 但目前只有TaskSchedulerlmpl这一个具体实现。 TaskScheduler 只为单个 Driver 调度任务。TaskSchedulerlmpl
  * 的功能包括接收 DAGScheduler 给每个Stage创建的Task集合，按照调度算法将资源分配给Task,将Task交给Spark集群
  * 不同节点上的Executor运行，在这些Task执行失败时进行重试，通过推断执行减轻落后的 Task对整体作业进度的影响。
  * <br>
  * Spark的资源调度分为两层：
  * <br>
  * 1、第一层是 Cluster Manager (在 YARN 模式下为 Resource Manager,在 Mesos 模式下为 Mesos Master,
  * 在 Standalone 模式下为 Master)将资源分配给 Application;
  * <br>
  * 2、第二层是 Application进一步将资源分配给 Application的各个Task。TaskSchedulerlmpl中的资源调度就是
  * 第二层的资源调度。
  * <br><br>
 * Low-level task scheduler interface, currently implemented exclusively by
 * [[org.apache.spark.scheduler.TaskSchedulerImpl]].
 * This interface allows plugging in different task schedulers. Each TaskScheduler schedules tasks
 * for a single SparkContext. These schedulers get sets of tasks submitted to them from the
 * DAGScheduler for each stage, and are responsible for sending the tasks to the cluster, running
 * them, retrying if there are failures, and mitigating stragglers. They return events to the
 * DAGScheduler.
 */
private[spark] trait TaskScheduler {

  private val appId = "spark-application-" + System.currentTimeMillis

  def rootPool: Pool

  def schedulingMode: SchedulingMode

  def start(): Unit

  // Invoked after system has successfully initialized (typically in spark context).
  // Yarn uses this to bootstrap allocation of resources based on preferred locations,
  // wait for slave registrations, etc.
  def postStartHook() { }

  // Disconnect from the cluster.
  def stop(): Unit

  // Submit a sequence of tasks to run.
  def submitTasks(taskSet: TaskSet): Unit

  // Kill all the tasks in a stage and fail the stage and all the jobs that depend on the stage.
  // Throw UnsupportedOperationException if the backend doesn't support kill tasks.
  def cancelTasks(stageId: Int, interruptThread: Boolean): Unit

  /**
   * Kills a task attempt.
   * Throw UnsupportedOperationException if the backend doesn't support kill a task.
   *
   * @return Whether the task was successfully killed.
   */
  def killTaskAttempt(taskId: Long, interruptThread: Boolean, reason: String): Boolean

  // Kill all the running task attempts in a stage.
  // Throw UnsupportedOperationException if the backend doesn't support kill tasks.
  def killAllTaskAttempts(stageId: Int, interruptThread: Boolean, reason: String): Unit

  // Set the DAG scheduler for upcalls. This is guaranteed to be set before submitTasks is called.
  def setDAGScheduler(dagScheduler: DAGScheduler): Unit

  // Get the default level of parallelism to use in the cluster, as a hint for sizing jobs.
  def defaultParallelism(): Int

  /**
   * Update metrics for in-progress tasks and let the master know that the BlockManager is still
   * alive. Return true if the driver knows about the given block manager. Otherwise, return false,
   * indicating that the block manager should re-register.
   */
  def executorHeartbeatReceived(
      execId: String,
      accumUpdates: Array[(Long, Seq[AccumulatorV2[_, _]])],
      blockManagerId: BlockManagerId): Boolean

  /**
   * Get an application ID associated with the job.
   *
   * @return An application ID
   */
  def applicationId(): String = appId

  /**
   * Process a lost executor
   */
  def executorLost(executorId: String, reason: ExecutorLossReason): Unit

  /**
   * Process a removed worker
   */
  def workerRemoved(workerId: String, host: String, message: String): Unit

  /**
   * Get an application's attempt ID associated with the job.
   *
   * @return An application's Attempt ID
   */
  def applicationAttemptId(): Option[String]

}
