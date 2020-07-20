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

import org.apache.spark.SparkException

/**
 * A factory class to create the [[RpcEnv]]. It must have an empty constructor so that it can be
 * created using Reflection.
 */
private[spark] trait RpcEnvFactory {

  def create(config: RpcEnvConfig): RpcEnv
}

/**
 * An end point for the RPC that defines what functions to trigger given a message.
 *
 * It is guaranteed that `onStart`, `receive` and `onStop` will be called in sequence.
 *
 * The life-cycle of an endpoint is:
 *
 * {@code constructor -> onStart -> receive* -> onStop}
 *
 * Note: `receive` can be called concurrently. If you want `receive` to be thread-safe, please use
 * [[ThreadSafeRpcEndpoint]]
 *
 * If any error is thrown from one of [[RpcEndpoint]] methods except `onError`, `onError` will be
 * invoked with the cause. If `onError` throws an error, [[RpcEnv]] will ignore it.
 */
/*
  5.3.1 RPC端点RpcEndpoint

  RPC端点是对Spark的RPC通信实体的统一抽象，所有运行于RPC框架之上的实体都应该继承RpcEndpoint。
  Spark早期版本节点间的消息通信主要采用Akka的Actor，从Spark 2.0.0版本开始移除了对Akka的依赖，
  这就意味着Spark需要Actor的替代品，RPC端点RpcEndpoint由此而生。RpcEndpoint是对能够处理RPC请求，
  给某一特定服务提供本地调用及跨节点调用的RPC组件的抽象。
 */
private[spark] trait RpcEndpoint {

  /**
   * The [[RpcEnv]] that this [[RpcEndpoint]] is registered to.
   * <br><br>
   * 当前RpcEndpoint所属的RpcEnv。
   */
  val rpcEnv: RpcEnv

  /**
   * The [[RpcEndpointRef]] of this [[RpcEndpoint]]. `self` will become valid when `onStart` is
   * called. And `self` will become `null` when `onStop` is called.
   *
   * Note: Because before `onStart`, [[RpcEndpoint]] has not yet been registered and there is not
   * valid [[RpcEndpointRef]] for it. So don't call `self` before `onStart` is called.
   * <br><br>
   *
   * 获取RpcEndpoint相关联的RpcEndpointRef。从代码实现看到，其实现实际调用了RpcEnv的endpointRef方法。
   * 由于RpcEnv并未实现此方法，所以需要RpcEnv的子类来实现。以NettyRpcEnv为例，其endpointRef方法的实现
   * 如代码清单5-40所示。
   */
  final def self: RpcEndpointRef = {
    require(rpcEnv != null, "rpcEnv has not been initialized")
    rpcEnv.endpointRef(this)
  }

  /**
   * Process messages from `RpcEndpointRef.send` or `RpcCallContext.reply`. If receiving a
   * unmatched message, `SparkException` will be thrown and sent to `onError`.
   * <br><br>
   * 接收消息并处理，但不需要给客户端回复
   */
  def receive: PartialFunction[Any, Unit] = {
    case _ => throw new SparkException(self + " does not implement 'receive'")
  }

  /**
   * Process messages from `RpcEndpointRef.ask`. If receiving a unmatched message,
   * `SparkException` will be thrown and sent to `onError`.
   *<br><br>
   * 接收消息并处理，需要给客户端回复。回复是通过 RpcCallContext
   */
  def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case _ => context.sendFailure(new SparkException(self + " won't reply anything"))
  }

  /**
   * Invoked when any exception is thrown during handling messages.
   * <br><br>
   * 当处理消息发生异常时调用，可以对异常进行一些处理。
   */
  def onError(cause: Throwable): Unit = {
    // By default, throw e and let RpcEnv handle it
    throw cause
  }

  /**
   * Invoked when `remoteAddress` is connected to the current node.
   * 当客户端与当前节点连接上之后调用，可以针对连接进行一些处理。
   */
  def onConnected(remoteAddress: RpcAddress): Unit = {
    // By default, do nothing.
  }

  /**
   * Invoked when `remoteAddress` is lost.
   *
   * 当客户端与当前节点的连接断开之后调用，可以针对断开连接进行一些处理。
   */
  def onDisconnected(remoteAddress: RpcAddress): Unit = {
    // By default, do nothing.
  }

  /**
   * Invoked when some network error happens in the connection between the current node and
   * `remoteAddress`.
   */
  def onNetworkError(cause: Throwable, remoteAddress: RpcAddress): Unit = {
    // By default, do nothing.
  }

  /**
   * Invoked before [[RpcEndpoint]] starts to handle any message.
   */
  def onStart(): Unit = {
    // By default, do nothing.
  }

  /**
   * Invoked when [[RpcEndpoint]] is stopping. `self` will be `null` in this method and you cannot
   * use it to send or ask messages.
   */
  def onStop(): Unit = {
    // By default, do nothing.
  }

  /**
   * A convenient method to stop [[RpcEndpoint]].
   */
  final def stop(): Unit = {
    val _self = self
    if (_self != null) {
      rpcEnv.stop(_self)
    }
  }
}

/**
 * A trait that requires RpcEnv thread-safely sending messages to it.
 *
 * Thread-safety means processing of one message happens before processing of the next message by
 * the same [[ThreadSafeRpcEndpoint]]. In the other words, changes to internal fields of a
 * [[ThreadSafeRpcEndpoint]] are visible when processing the next message, and fields in the
 * [[ThreadSafeRpcEndpoint]] need not be volatile or equivalent.
 *
 * However, there is no guarantee that the same thread will be executing the same
 * [[ThreadSafeRpcEndpoint]] for different messages.
 *
 * 主要用于对消息的处理，必须是线程安全的场景。ThreadSafeRpcEndpoint对消息的处理都是串行的，
 * 即前一条消息处理完才能接着处理下一条消息。
 */
private[spark] trait ThreadSafeRpcEndpoint extends RpcEndpoint
