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

/**
  * SchedulerBackend是 TaskScheduler 的调度后端接口。TaskScheduler给Task分配资源
  * 实际是通过SchedulerBackend来完成的，SchedulerBackend给Task分配完资源后将与分配给
  * Task的Executor通信，并要求后者运行Task。<br><br>
  *
  * 文档：SchedulerBackend 的继承体系.note
  * 链接：http://note.youdao.com/noteshare?id=b1c81528f478b703a85e7800299a018f&sub=08C78887FCA34B34B6B69119CDC37E72
  *
 * A backend interface for scheduling systems that allows plugging in different ones under
 * TaskSchedulerImpl. We assume a Mesos-like model where the application gets resource offers as
 * machines become available and can launch tasks on them.
 */
private[spark] trait SchedulerBackend {
  /**
    * 与当前 Job 关联的app的身份标识
    */
  private val appId = "spark-application-" + System.currentTimeMillis

  def start(): Unit
  def stop(): Unit
  def reviveOffers(): Unit
  def defaultParallelism(): Int

  /**
   * Requests that an executor kills a running task.
   *
   * @param taskId Id of the task.
   * @param executorId Id of the executor the task is running on.
   * @param interruptThread Whether the executor should interrupt the task thread.
   * @param reason The reason for the task kill.
   */
  def killTask(
      taskId: Long,
      executorId: String,
      interruptThread: Boolean,
      reason: String): Unit =
    throw new UnsupportedOperationException

  def isReady(): Boolean = true

  /**
   * Get an application ID associated with the job.
   *
   * @return An application ID
   */
  def applicationId(): String = appId

  /**
    * 当应用在duster模式运行且集群管理器支持应用进行多次执 行尝试时，此方法可以获取应用程序尝试的标识。
    * 当应用程序在client模式运行时， 将不支持多次尝试，因此此方法不会获取到应用程序尝试的标识。
    * <br> <br>
    *
   * Get the attempt ID for this run, if the cluster manager supports multiple
   * attempts. Applications run in client mode will not have attempt IDs.
   *
   * @return The application attempt id, if available.
   */
  def applicationAttemptId(): Option[String] = None

  /**
   * Get the URLs for the driver logs. These URLs are used to display the links in the UI
   * Executors tab for the driver.
   * @return Map containing the log names and their respective URLs
   */
  def getDriverLogUrls: Option[Map[String, String]] = None

  /**
   * Get the max number of tasks that can be concurrent launched currently.
   * Note that please don't cache the value returned by this method, because the number can change
   * due to add/remove executors.
   *
   * @return The max number of tasks that can be concurrent launched currently.
   */
  def maxNumConcurrentTasks(): Int

}
