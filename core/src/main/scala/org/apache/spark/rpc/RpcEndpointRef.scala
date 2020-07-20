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

package org.apache.spark.rpc

import scala.concurrent.Future
import scala.reflect.ClassTag

import org.apache.spark.{SparkConf, SparkException}
import org.apache.spark.internal.Logging
import org.apache.spark.util.RpcUtils

/**
 * A reference for a remote [[RpcEndpoint]]. [[RpcEndpointRef]] is thread-safe.
 * <br><br>
 * 如果说RpcEndpoint是Akka中Actor的替代产物，那么RpcEndpointRef就是Actor-Ref的替代产物。
 * 在Akka中只要你持有了一个Actor的引用ActorRef，那么你就可以使用此ActorRef向远端的Actor
 * 发起请求。RpcEndpointRef也具有同等的效用，要向一个远端的RpcEndpoint发起请求，你就必须
 * 持有这个RpcEndpoint的RpcEndpointRef。
 *
 * RPC端点引用，即RPC分布式环境中一个具体实体的引用，所谓引用实际是“spark://host:port/name”
 * 这种格式的地址。其中，
 * host为端点所在RPC服务所在的主机IP,
 * port是端点所在RPC服务的端口，
 * name是端点实例的名称。
 *
 * address + name
 */

private[spark] abstract class RpcEndpointRef(conf: SparkConf)
  extends Serializable with Logging {
  // RPC最大重新连接次数。可以使用spark.rpc.numRetries属性进行配置，默认为3次。
  private[this] val maxRetries = RpcUtils.numRetries(conf)
  // RPC每次重新连接需要等待的毫秒数。可以使用spark.rpc.retry.wait属性进行配置，默认值为3秒。
  private[this] val retryWaitMs = RpcUtils.retryWaitMs(conf)
  /*
     RPC的ask操作的默认超时时间。可以使用spark.rpc.askTimeout或者spark.network.timeout
     属性进行配置，默认值为120秒。spark.rpc.askTimeout属性的优先级更高。
   */
  private[this] val defaultAskTimeout = RpcUtils.askRpcTimeout(conf)

  /**
   * return the address for the [[RpcEndpointRef]]
   * <br>
   * 回当前RpcEndpointRef对应RpcEndpoint的RPC地址（RpcAddress）。
   */
  def address: RpcAddress
  // 返回当前RpcEndpointRef对应RpcEndpoint的名称。
  def name: String

  /**
   * Sends a one-way asynchronous message. Fire-and-forget semantics.
   * 发送单向异步的消息。所谓“单向”就是发送完后就会忘记此次发送，不会有任何状态要记录，
   * 也不会期望得到服务端的回复。send采用了at-most-once的投递规则。RpcEndpointRef的
   * send方法非常类似于Akka中Actor的tell方法。
   */
  def send(message: Any): Unit

  /**
   * Send a message to the corresponding [[RpcEndpoint.receiveAndReply)]] and return a [[Future]] to
   * receive the reply within the specified timeout.
   *
   * This method only sends the message once and never retries.
   * 以默认的超时时间作为timeout参数
   */
  def ask[T: ClassTag](message: Any, timeout: RpcTimeout): Future[T]

  /**
   * Send a message to the corresponding [[RpcEndpoint.receiveAndReply)]] and return a [[Future]] to
   * receive the reply within a default timeout.
   *
   * This method only sends the message once and never retries.
   */
  def ask[T: ClassTag](message: Any): Future[T] = ask(message, defaultAskTimeout)

  /**
   * Send a message to the corresponding [[RpcEndpoint.receiveAndReply]] and get its result within a
   * default timeout, throw an exception if this fails.
   *
   * Note: this is a blocking action which may cost a lot of time,  so don't call it in a message
   * loop of [[RpcEndpoint]].

   * @param message the message to send
   * @tparam T type of the reply message
   * @return the reply message from the corresponding [[RpcEndpoint]]
   */
    /*
      发送同步的请求，此类请求将会被RpcEndpoint接收，并在指定的超时时间内等待返回类型为T的处理结果。
      当此方法抛出SparkException时，将会进行请求重试，直到超过了默认的重试次数为止。由于此类方法会重试，
      因此要求服务端对消息的处理是幂等的。此方法也采用了at-least-once的投递规则。此方法也非常类似于Akka
      中采用了at-least-once机制的Actor的ask方法。
     */
  def askSync[T: ClassTag](message: Any): T = askSync(message, defaultAskTimeout)

  /**
   * Send a message to the corresponding [[RpcEndpoint.receiveAndReply]] and get its result within a
   * specified timeout, throw an exception if this fails.
   *
   * Note: this is a blocking action which may cost a lot of time, so don't call it in a message
   * loop of [[RpcEndpoint]].
   *
   * @param message the message to send
   * @param timeout the timeout duration
   * @tparam T type of the reply message
   * @return the reply message from the corresponding [[RpcEndpoint]]
   */
  def askSync[T: ClassTag](message: Any, timeout: RpcTimeout): T = {
    val future = ask[T](message, timeout)
    timeout.awaitResult(future)
  }

}
