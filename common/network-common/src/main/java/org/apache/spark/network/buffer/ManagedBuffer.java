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

package org.apache.spark.network.buffer;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

/**
 * Managed Buffer提供了由字节构成数据的不可变视图
 * 也就是说 Managed Buffer并不存储数据， 也不是数据的实际来源， 这同关系型数据库的视图类似) 。
 *
 * This interface provides an immutable view for data in the form of bytes. The implementation
 * should specify how the data is provided:
 *
 * - {@link FileSegmentManagedBuffer}: data backed by part of a file
 * - {@link NioManagedBuffer}: data backed by a NIO ByteBuffer
 * - {@link NettyManagedBuffer}: data backed by a Netty ByteBuf
 *
 * The concrete buffer implementation might be managed outside the JVM garbage collector.
 * For example, in the case of {@link NettyManagedBuffer}, the buffers are reference counted.
 * In that case, if the buffer is going to be passed around to a different thread, retain/release
 * should be called.
 */
public abstract class ManagedBuffer {

  /**
   * 返回数据的字节数
   *
   * Number of bytes of the data. If this buffer will decrypt for all of the views into the data,
   * this is the size of the decrypted data.
   */
  public abstract long size();

  /**
   * 将数据按照NIO的 ByteBuffer类型返回
   *
   * Exposes this buffer's data as an NIO ByteBuffer. Changing the position and limit of the
   * returned ByteBuffer should not affect the content of this buffer.
   */
  // TODO: Deprecate this, usage may require expensive memory mapping or allocation.
  public abstract ByteBuffer nioByteBuffer() throws IOException;

  /**
   * 将数据按照 InputStream返回
   *
   * Exposes this buffer's data as an InputStream. The underlying implementation does not
   * necessarily check for the length of bytes read, so the caller is responsible for making sure
   * it does not go over the limit.
   */
  public abstract InputStream createInputStream() throws IOException;

  /**
   * 当有新的使用者使用此视图时,增加引用此视图的引用数
   * Increment the reference count by one if applicable.
   */
  public abstract ManagedBuffer retain();

  /**
   * 当有使用者不再使用此视图时,减少引用此视图的引用数。当引用数为0
   * 时释放缓冲区
   *
   * If applicable, decrement the reference count by one and deallocates the buffer if the
   * reference count reaches zero.
   */
  public abstract ManagedBuffer release();

  /**
   * 将缓冲区的数据转换为Netty对象,用来将数据写到外部
   *
   * Convert the buffer into an Netty object, used to write the data out. The return value is either
   * a {@link io.netty.buffer.ByteBuf} or a {@link io.netty.channel.FileRegion}.
   *
   * If this method returns a ByteBuf, then that buffer's reference count will be incremented and
   * the caller will be responsible for releasing this new reference.
   */
  public abstract Object convertToNetty() throws IOException;
}
