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

package org.apache.spark.shuffle

import java.io._
import java.nio.channels.Channels
import java.nio.file.Files

import org.apache.spark.{SparkConf, SparkEnv}
import org.apache.spark.internal.Logging
import org.apache.spark.io.NioBufferedFileInputStream
import org.apache.spark.network.buffer.{FileSegmentManagedBuffer, ManagedBuffer}
import org.apache.spark.network.netty.SparkTransportConf
import org.apache.spark.shuffle.IndexShuffleBlockResolver.NOOP_REDUCE_ID
import org.apache.spark.storage._
import org.apache.spark.util.Utils

/**
  *
  * 特质ShuffleBlockResolver定义了对ShuffleBlock进行解析的规范， 包括获取Shuffe数
  * 据文件、获取Shuffle索引文件、删除指定的Shuffle数据文件和索引文件、生成S huff ie索
  * 引文件、获取Shu fle块的数据等。Shuffle Block Resolver目前只有IndexShuffleBlockResolver
  * 这唯一的实现类。IndexShuffleBlockResolver用于创建和维护ShuffleBlock与物理文件位置
  * 之间的映射关系。<br><br>
  *
  *
 * Create and maintain the shuffle blocks' mapping between logic block and physical file location.
 * Data of shuffle blocks from the same map task are stored in a single consolidated data file.
 * The offsets of the data blocks in the data file are stored in a separate index file.
 *
 * We use the name of the shuffle data's shuffleBlockId with reduce ID set to 0 and add ".data"
 * as the filename postfix for data file, and ".index" as the filename postfix for index file.
 *
 */
// Note: Changes to the format in this file should be kept in sync with
// org.apache.spark.network.shuffle.ExternalShuffleBlockResolver#getSortBasedShuffleBlockData().
private[spark] class IndexShuffleBlockResolver(
    conf: SparkConf,
    _blockManager: BlockManager = null)
  extends ShuffleBlockResolver
  with Logging {
  //  SparkEnv类方法 SparkEnv.get.blockManager
  private lazy val blockManager = Option(_blockManager).getOrElse(SparkEnv.get.blockManager)
  /**
    * transportConf：即与Shuffle相关的transportConf。有了transportConf， 就可以方
    * 便地对Shuffle客户端传输线程数(spark.shuffle.io.client Threads属性) 和Shuffle服
    * 务端传输线程数(spark.shuffle.io.server Threads属性) 进行读取。TtransportConf已
    * 在3.2.1节详细介绍过，此处不再赘述。
    */
  private val transportConf = SparkTransportConf.fromSparkConf(conf, "shuffle")
  /**
    *  从文件夹中找 "shuffle_" + shuffleId + "_" + mapId + "_" + reduceId + ".data" 的文件
    */
  def getDataFile(shuffleId: Int, mapId: Int): File = {
    blockManager.diskBlockManager.getFile(ShuffleDataBlockId(shuffleId, mapId, NOOP_REDUCE_ID))
  }

  /**
    * 从文件夹中找 "shuffle_" + shuffleId + "_" + mapId + "_" + reduceId + ".index" 的文件
    */
  private def getIndexFile(shuffleId: Int, mapId: Int): File = {
    blockManager.diskBlockManager.getFile(ShuffleIndexBlockId(shuffleId, mapId, NOOP_REDUCE_ID))
  }

  /**
   * 用于删除Shuffle过程中包含指定map任务输出数据的Shuffle数据文件和索引文件。
   */
  def removeDataByMap(shuffleId: Int, mapId: Int): Unit = {
    var file = getDataFile(shuffleId, mapId)
    if (file.exists()) {
      if (!file.delete()) {
        logWarning(s"Error deleting data ${file.getPath()}")
      }
    }

    file = getIndexFile(shuffleId, mapId)
    if (file.exists()) {
      if (!file.delete()) {
        logWarning(s"Error deleting index ${file.getPath()}")
      }
    }
  }

  /**
    * 对给定的索引文件和数据文件是否匹配进行检查
    * 主要通过对数据文件的长度和索引文件偏移量的和进行比较来实现 <br>
    *   
    *   dataFile 块的个数
    *
   * Check whether the given index and data files match each other.
   * If so, return the partition lengths in the data file. Otherwise return null.
   */
  private def checkIndexAndDataFile(index: File, data: File, blocks: Int): Array[Long] = {
    // the index file should have `block + 1` longs as offset.
    // TODO: 为什么？
    if (index.length() != (blocks + 1) * 8L) {
      return null

    }
    val lengths = new Array[Long](blocks)
    // Read the lengths of blocks
    val in = try {
      new DataInputStream(new NioBufferedFileInputStream(index))
    } catch {
      case e: IOException =>
        return null
    }
    try {
      // Convert the offsets into lengths of each block
      var offset = in.readLong()
      if (offset != 0L) {
        return null
      }
      var i = 0
      while (i < blocks) {
        val off = in.readLong()
        lengths(i) = off - offset
        offset = off
        i += 1
      }
    } catch {
      case e: IOException =>
        return null
    } finally {
      in.close()
    }

    // the size of data file should match with index file
    if (data.length() == lengths.sum) {
      lengths
    } else {
      null
    }
  }

  /**
   * Write an index file with the offsets of each block, plus a final offset at the end for the
   * end of the output file. This will be used by getBlockData to figure out where each block
   * begins and ends.
   *
   * It will commit the data and index file as an atomic operation, use the existing ones, or
   * replace them with new ones.
   *
   * Note: the `lengths` will be updated to match the existing index file if use the existing ones.
   */
  def writeIndexFileAndCommit(
      shuffleId: Int,
      mapId: Int,
      lengths: Array[Long],
      dataTmp: File): Unit = {
    /*
       dataTmp 临时文件
       获取指定Shuffle中指定map任务输出的索引文件,没有则会创建
     */
    val indexFile = getIndexFile(shuffleId, mapId)
    // 根据索引文件,获取临时索引文件的路径, 和索引文件在同一目录
    val indexTmp = Utils.tempFileWith(indexFile)
    try {
      // 指定map任务输出的数据文件
      val dataFile = getDataFile(shuffleId, mapId)
      // There is only one IndexShuffleBlockResolver per executor, this synchronization make sure
      // the following check and rename are atomic.
      synchronized {
        /*
            对给定的索引文件和数据文件是否匹配进行检查。 checkIndexAndDataFile方法最后将返回
            existingLengths = 索引文件中各个partition的长度数据existingLengths

            lengths ：索引文件中各个partition的长度数据
         */
        val existingLengths = checkIndexAndDataFile(indexFile, dataFile, lengths.length)
        if (existingLengths != null) {
          // Another attempt for the same task has already written our map outputs successfully,
          // so just use the existing partition lengths and delete our temporary map outputs.
          System.arraycopy(existingLengths, 0, lengths, 0, lengths.length)
          if (dataTmp != null && dataTmp.exists()) {
            dataTmp.delete()
          }
        } else {
          /*
              说明是为map任务中间结果输出的第一次成功尝试,因而需要将
              indexTmp 重命名为 indexFile,将 dataTmp 重命名为 dataFile
           */
          // This is the first successful attempt in writing the map outputs for this task,
          // so override any existing index and data files with the ones we wrote.
          val out = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(indexTmp)))
          // 写入索引
          Utils.tryWithSafeFinally {
            // We take in lengths of each block, need to convert it to offsets.
            var offset = 0L
            out.writeLong(offset)
            for (length <- lengths) {
              offset += length
              out.writeLong(offset)
            }
          } {
            out.close()
          }
          /*
              删除已有的
           */
          if (indexFile.exists()) {
            indexFile.delete()
          }
          if (dataFile.exists()) {
            dataFile.delete()
          }
          //  在同一个目录更改 name ,即替换
          if (!indexTmp.renameTo(indexFile)) {
            throw new IOException("fail to rename file " + indexTmp + " to " + indexFile)
          }
          if (dataTmp != null && dataTmp.exists() && !dataTmp.renameTo(dataFile)) {
            throw new IOException("fail to rename file " + dataTmp + " to " + dataFile)
          }
        }
      }
    } finally {
      if (indexTmp.exists() && !indexTmp.delete()) {
        logError(s"Failed to delete temporary index file at ${indexTmp.getAbsolutePath}")
      }
    }
  }

  /**
    * 获取 ShuffleBlockId 对应的数据
    * @param blockId
    * @return
    */
  override def getBlockData(blockId: ShuffleBlockId): ManagedBuffer = {
    // The block is actually going to be a range of a single map output file for this map, so
    // find out the consolidated file, then the offset within that from our index
    val indexFile = getIndexFile(blockId.shuffleId, blockId.mapId)

    // SPARK-22982: if this FileInputStream's position is seeked forward by another piece of code
    // which is incorrectly using our file descriptor then this code will fetch the wrong offsets
    // (which may cause a reducer to be sent a different reducer's data). The explicit position
    // checks added here were a useful debugging aid during SPARK-22982 and may help prevent this
    // class of issue from re-occurring in the future which is why they are left here even though
    // SPARK-22982 is fixed.
    val channel = Files.newByteChannel(indexFile.toPath)
    /*
       channel.position 移动的单位是 byte , 而 long = 8 byte,
       定位到 reduceId 的对应的数据
     */
    channel.position(blockId.reduceId * 8L)
    val in = new DataInputStream(Channels.newInputStream(channel))
    try {
      /*
         Java 中 Long 是 64 bit = 8 bytes

         This channel's position, a non-negative integer counting the number of bytes
         from the beginning of the entity to the current position

         readLong 读取一个Long的长度的数据
       */
      val offset = in.readLong()
      val nextOffset = in.readLong()
      val actualPosition = channel.position()
      val expectedPosition = blockId.reduceId * 8L + 16
      if (actualPosition != expectedPosition) {
        throw new Exception(s"SPARK-22982: Incorrect channel position after index file reads: " +
          s"expected $expectedPosition but actual position was $actualPosition.")
      }
      /*
          包裹了文件路径，和偏移的 ManagedBuffer
       */
      new FileSegmentManagedBuffer(
        transportConf,
        getDataFile(blockId.shuffleId, blockId.mapId),
        offset,
        nextOffset - offset)
    } finally {
      in.close()
    }
  }

  override def stop(): Unit = {}
}

private[spark] object IndexShuffleBlockResolver {
  // No-op reduce ID used in interactions with disk store.
  // The disk store currently expects puts to relate to a (map, reduce) pair, but in the sort
  // shuffle outputs for several reduces are glommed into a single file.
  val NOOP_REDUCE_ID = 0
}
