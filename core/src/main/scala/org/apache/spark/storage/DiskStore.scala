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

package org.apache.spark.storage

import java.io._
import java.nio.ByteBuffer
import java.nio.channels.{Channels, ReadableByteChannel, WritableByteChannel}
import java.nio.channels.FileChannel.MapMode
import java.util.concurrent.ConcurrentHashMap

import scala.collection.mutable.ListBuffer

import com.google.common.io.Closeables
import io.netty.channel.DefaultFileRegion

import org.apache.spark.{SecurityManager, SparkConf}
import org.apache.spark.internal.{config, Logging}
import org.apache.spark.network.buffer.ManagedBuffer
import org.apache.spark.network.util.{AbstractFileRegion, JavaUtils}
import org.apache.spark.security.CryptoStreamUtils
import org.apache.spark.unsafe.array.ByteArrayMethods
import org.apache.spark.util.Utils
import org.apache.spark.util.io.ChunkedByteBuffer

/**
 *  DiskStore负责将Bock存储到磁盘。由于 DiskStore依赖于 DiskBlockManager的服
  * 务,所以我们在上一节先介绍了 Disk BlockManager。在 Spark1x.x版本中, Block Store提
  * 供了对磁盘存储 DiskStore、内存存储 Memory Store及 Tachyon(即 Alluxio)存储 Tachyon
  * Store的统一规范, DiskStore、 Memory Store和 TachyonStore都是具体的实现。但从 Spark
  * 2.0.0版本开始,取消了 Tachyon Store,而且也不再使用 BlockStore提供统一的接口规范,
  * Disk Store和 Memory Store都是分别实现的。
 */
private[spark] class DiskStore(
    conf: SparkConf,
    diskManager: DiskBlockManager,
    securityManager: SecurityManager) extends Logging {
  /**
    * 读取磁盘中的 Block时,是直接读取还是使用FileChannel的内存镜像映射方法读取的阈值。
    */
    /*
        小贴士:什么是 File Channel的内存镜像映射方法?在 Java NIo中, File Channe的
        map方法所提供的快速读写技术,其实质上是将通道所连接的数据节点中的全部或部分
        数据直接映射到内存的一个Buffer中,而这个内存 Buffer块就是节点数据的映像,你直
        接对这个 Buffer进行修改,会影响到节点数据。这个 Buffer叫做 MappedBuffer,即镜像
        Buffer。由于是内存镜像,因此处理速度快。

        MappedByteBuffer  https://www.cnblogs.com/guozp/p/10470431.html
     */
  private val minMemoryMapBytes = conf.getSizeAsBytes("spark.storage.memoryMapThreshold", "2m")
  private val maxMemoryMapBytes = conf.get(config.MEMORY_MAP_LIMIT_FOR_TESTS)
  private val blockSizes = new ConcurrentHashMap[BlockId, Long]()

  def getSize(blockId: BlockId): Long = blockSizes.get(blockId)

  /**
   * Invokes the provided callback function to write the specific block.
   *
   * @throws IllegalStateException if the block already exists in the disk store.
   */
  def put(blockId: BlockId)(writeFunc: WritableByteChannel => Unit): Unit = {
    if (contains(blockId)) {
      throw new IllegalStateException(s"Block $blockId is already present in the disk store")
    }
    logDebug(s"Attempting to put block $blockId")
    val startTime = System.currentTimeMillis
    val file = diskManager.getFile(blockId)
    val out = new CountingWritableChannel(openForWrite(file))
    var threwException: Boolean = true
    try {
      writeFunc(out)
      blockSizes.put(blockId, out.getCount)
      threwException = false
    } finally {
      try {
        out.close()
      } catch {
        case ioe: IOException =>
          if (!threwException) {
            threwException = true
            throw ioe
          }
      } finally {
         if (threwException) {
          remove(blockId)
        }
      }
    }
    val finishTime = System.currentTimeMillis
    logDebug("Block %s stored as %s file on disk in %d ms".format(
      file.getName,
      Utils.bytesToString(file.length()),
      finishTime - startTime))
  }

  def putBytes(blockId: BlockId, bytes: ChunkedByteBuffer): Unit = {
    put(blockId) { channel =>
      bytes.writeFully(channel)
    }
  }

  def getBytes(blockId: BlockId): BlockData = {
    val file = diskManager.getFile(blockId.name)
    val blockSize = getSize(blockId)

    securityManager.getIOEncryptionKey() match {
      case Some(key) =>
        // Encrypted blocks cannot be memory mapped; return a special object that does decryption
        // and provides InputStream / FileRegion implementations for reading the data.
        new EncryptedBlockData(file, blockSize, conf, key)

      case _ =>
        new DiskBlockData(minMemoryMapBytes, maxMemoryMapBytes, file, blockSize)
    }
  }

  def remove(blockId: BlockId): Boolean = {
    blockSizes.remove(blockId)
    val file = diskManager.getFile(blockId.name)
    if (file.exists()) {
      val ret = file.delete()
      if (!ret) {
        logWarning(s"Error deleting ${file.getPath()}")
      }
      ret
    } else {
      false
    }
  }

  def contains(blockId: BlockId): Boolean = {
    val file = diskManager.getFile(blockId.name)
    file.exists()
  }

  private def openForWrite(file: File): WritableByteChannel = {
    val out = new FileOutputStream(file).getChannel()
    try {
      securityManager.getIOEncryptionKey().map { key =>
        CryptoStreamUtils.createWritableChannel(out, conf, key)
      }.getOrElse(out)
    } catch {
      case e: Exception =>
        Closeables.close(out, true)
        file.delete()
        throw e
    }
  }

}

private class DiskBlockData(
    minMemoryMapBytes: Long,
    maxMemoryMapBytes: Long,
    file: File,
    blockSize: Long) extends BlockData {

  override def toInputStream(): InputStream = new FileInputStream(file)

  /**
  * Returns a Netty-friendly wrapper for the block's data.
  *
  * Please see `ManagedBuffer.convertToNetty()` for more details.
  */
  override def toNetty(): AnyRef = new DefaultFileRegion(file, 0, size)

  override def toChunkedByteBuffer(allocator: (Int) => ByteBuffer): ChunkedByteBuffer = {
    Utils.tryWithResource(open()) { channel =>
      var remaining = blockSize
      val chunks = new ListBuffer[ByteBuffer]()
      while (remaining > 0) {
        val chunkSize = math.min(remaining, maxMemoryMapBytes)
        val chunk = allocator(chunkSize.toInt)
        remaining -= chunkSize
        JavaUtils.readFully(channel, chunk)
        chunk.flip()
        chunks += chunk
      }
      new ChunkedByteBuffer(chunks.toArray)
    }
  }

  override def toByteBuffer(): ByteBuffer = {
    require(blockSize < maxMemoryMapBytes,
      s"can't create a byte buffer of size $blockSize" +
      s" since it exceeds ${Utils.bytesToString(maxMemoryMapBytes)}.")
    Utils.tryWithResource(open()) { channel =>
      if (blockSize < minMemoryMapBytes) {
        // For small files, directly read rather than memory map.
        val buf = ByteBuffer.allocate(blockSize.toInt)
        JavaUtils.readFully(channel, buf)
        buf.flip()
        buf
      } else {
        channel.map(MapMode.READ_ONLY, 0, file.length)
      }
    }
  }

  override def size: Long = blockSize

  override def dispose(): Unit = {}

  private def open() = new FileInputStream(file).getChannel
}

private[spark] class EncryptedBlockData(
    file: File,
    blockSize: Long,
    conf: SparkConf,
    key: Array[Byte]) extends BlockData {

  override def toInputStream(): InputStream = Channels.newInputStream(open())

  override def toNetty(): Object = new ReadableChannelFileRegion(open(), blockSize)

  override def toChunkedByteBuffer(allocator: Int => ByteBuffer): ChunkedByteBuffer = {
    val source = open()
    try {
      var remaining = blockSize
      val chunks = new ListBuffer[ByteBuffer]()
      while (remaining > 0) {
        val chunkSize = math.min(remaining, ByteArrayMethods.MAX_ROUNDED_ARRAY_LENGTH)
        val chunk = allocator(chunkSize.toInt)
        remaining -= chunkSize
        JavaUtils.readFully(source, chunk)
        chunk.flip()
        chunks += chunk
      }

      new ChunkedByteBuffer(chunks.toArray)
    } finally {
      source.close()
    }
  }

  override def toByteBuffer(): ByteBuffer = {
    // This is used by the block transfer service to replicate blocks. The upload code reads
    // all bytes into memory to send the block to the remote executor, so it's ok to do this
    // as long as the block fits in a Java array.
    assert(blockSize <= ByteArrayMethods.MAX_ROUNDED_ARRAY_LENGTH,
      "Block is too large to be wrapped in a byte buffer.")
    val dst = ByteBuffer.allocate(blockSize.toInt)
    val in = open()
    try {
      JavaUtils.readFully(in, dst)
      dst.flip()
      dst
    } finally {
      Closeables.close(in, true)
    }
  }

  override def size: Long = blockSize

  override def dispose(): Unit = { }

  private def open(): ReadableByteChannel = {
    val channel = new FileInputStream(file).getChannel()
    try {
      CryptoStreamUtils.createReadableChannel(channel, conf, key)
    } catch {
      case e: Exception =>
        Closeables.close(channel, true)
        throw e
    }
  }
}

private[spark] class EncryptedManagedBuffer(
    val blockData: EncryptedBlockData) extends ManagedBuffer {

  // This is the size of the decrypted data
  override def size(): Long = blockData.size

  override def nioByteBuffer(): ByteBuffer = blockData.toByteBuffer()

  override def convertToNetty(): AnyRef = blockData.toNetty()

  override def createInputStream(): InputStream = blockData.toInputStream()

  override def retain(): ManagedBuffer = this

  override def release(): ManagedBuffer = this
}

private class ReadableChannelFileRegion(source: ReadableByteChannel, blockSize: Long)
  extends AbstractFileRegion {

  private var _transferred = 0L

  private val buffer = ByteBuffer.allocateDirect(64 * 1024)
  buffer.flip()

  override def count(): Long = blockSize

  override def position(): Long = 0

  override def transferred(): Long = _transferred

  override def transferTo(target: WritableByteChannel, pos: Long): Long = {
    assert(pos == transferred(), "Invalid position.")

    var written = 0L
    var lastWrite = -1L
    while (lastWrite != 0) {
      if (!buffer.hasRemaining()) {
        buffer.clear()
        source.read(buffer)
        buffer.flip()
      }
      if (buffer.hasRemaining()) {
        lastWrite = target.write(buffer)
        written += lastWrite
      } else {
        lastWrite = 0
      }
    }

    _transferred += written
    written
  }

  override def deallocate(): Unit = source.close()
}

private class CountingWritableChannel(sink: WritableByteChannel) extends WritableByteChannel {

  private var count = 0L

  def getCount: Long = count

  override def write(src: ByteBuffer): Int = {
    val written = sink.write(src)
    if (written > 0) {
      count += written
    }
    written
  }

  override def isOpen(): Boolean = sink.isOpen()

  override def close(): Unit = sink.close()

}
