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

package org.apache.spark.network.netty

import java.nio.ByteBuffer

import scala.collection.JavaConverters._
import scala.language.existentials
import scala.reflect.ClassTag

import org.apache.spark.internal.Logging
import org.apache.spark.network.BlockDataManager
import org.apache.spark.network.buffer.NioManagedBuffer
import org.apache.spark.network.client.{RpcResponseCallback, StreamCallbackWithID, TransportClient}
import org.apache.spark.network.server.{OneForOneStreamManager, RpcHandler, StreamManager}
import org.apache.spark.network.shuffle.protocol._
import org.apache.spark.serializer.Serializer
import org.apache.spark.storage.{BlockId, StorageLevel}

/**
 * Serves requests to open blocks by simply registering one chunk per block requested.
 * Handles opening and uploading arbitrary BlockManager blocks.
 *
 * Opened blocks are registered with the "one-for-one" strategy, meaning each Transport-layer Chunk
 * is equivalent to one Spark-level shuffle block.
 */
class NettyBlockRpcServer(
    appId: String,
    serializer: Serializer,
    blockManager: BlockDataManager)
  extends RpcHandler with Logging {

  private val streamManager = new OneForOneStreamManager()

  override def receive(
      client: TransportClient,
      rpcMessage: ByteBuffer,
      responseContext: RpcResponseCallback): Unit = {
    val message = BlockTransferMessage.Decoder.fromByteBuffer(rpcMessage)
    logTrace(s"Received request: $message")

    message match {
        /*
        1) Open Blocks:打开(读取) Block。 Netty BlockRpcServer对这种消息的处理步骤如下。
            ①取出 Open Blocks消息携带的 Blockid数组。
            ②调用 BlockManager的 getBlockData方法,获取数组中每一个 Blockid对应的 Block
            (返回值为 ManagedBuffer的序列)。
            ③调用 OneForOneStreamManager的 register Stream方法,将 ManagedBuffer序列注册
            到 OneForOneStreamManager的 streams缓存。
            ④创建 StreamHandle消息(包含 streamId和 ManagedBuffer序列的大小),并通过响
            应上下文回复客户端。
         */
      case openBlocks: OpenBlocks =>
        val blocksNum = openBlocks.blockIds.length
        /*
          for循环中的 yield 会把当前的元素记下来，保存在集合中，循环结束后将返回该集合。
          Scala中for循环是有返回值的。如果被循环的是Map，返回的就是Map，
          被循环的是List，返回的就是List
         */
        val blocks = for (i <- (0 until blocksNum).view)
          yield blockManager.getBlockData(BlockId.apply(openBlocks.blockIds(i)))
        // streamManager   类型 为 OneForOneStreamManager
        val streamId = streamManager.registerStream(appId, blocks.iterator.asJava,
          client.getChannel)
        logTrace(s"Registered streamId $streamId with $blocksNum buffers")
        // 向客户端发送 response
        responseContext.onSuccess(new StreamHandle(streamId, blocksNum).toByteBuffer)
        /*
        2) UploadBlock:上传 BlockNetty BlockRpcServer对这种消息的处理步骤如下。
            ①对 UploadBlock消息携带的元数据 metadata进行反序列化,得到存储级别( StorageLevel)
            和类型标记(上传 Block的类型)。
            ②将 UploadBlock消息携带的Bock数据(即 blockData),封装为 NioManagedBuffer
            ③获取 UploadBlock消息携带的 Blockid。
            ④调用 BlockManager的 putBlockData方法,将Block存入本地存储体系。
            ⑤通过响应上下文回复客户端。
         */
      case uploadBlock: UploadBlock =>
        // StorageLevel and ClassTag are serialized as bytes using our JavaSerializer.
        val (level: StorageLevel, classTag: ClassTag[_]) = {
          serializer
            .newInstance()
            .deserialize(ByteBuffer.wrap(uploadBlock.metadata))
            .asInstanceOf[(StorageLevel, ClassTag[_])]
        }
        val data = new NioManagedBuffer(ByteBuffer.wrap(uploadBlock.blockData))
        val blockId = BlockId(uploadBlock.blockId)
        logDebug(s"Receiving replicated block $blockId with level ${level} " +
          s"from ${client.getSocketAddress}")
        blockManager.putBlockData(blockId, data, level, classTag)
        responseContext.onSuccess(ByteBuffer.allocate(0))
    }
  }

  override def receiveStream(
      client: TransportClient,
      messageHeader: ByteBuffer,
      responseContext: RpcResponseCallback): StreamCallbackWithID = {
    val message =
      BlockTransferMessage.Decoder.fromByteBuffer(messageHeader).asInstanceOf[UploadBlockStream]
    val (level: StorageLevel, classTag: ClassTag[_]) = {
      serializer
        .newInstance()
        .deserialize(ByteBuffer.wrap(message.metadata))
        .asInstanceOf[(StorageLevel, ClassTag[_])]
    }
    val blockId = BlockId(message.blockId)
    logDebug(s"Receiving replicated block $blockId with level ${level} as stream " +
      s"from ${client.getSocketAddress}")
    // This will return immediately, but will setup a callback on streamData which will still
    // do all the processing in the netty thread.
    blockManager.putBlockDataAsStream(blockId, level, classTag)
  }

  override def getStreamManager(): StreamManager = streamManager
}
