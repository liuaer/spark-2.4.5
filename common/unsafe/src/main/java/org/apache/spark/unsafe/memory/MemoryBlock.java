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

package org.apache.spark.unsafe.memory;

import javax.annotation.Nullable;

import org.apache.spark.unsafe.Platform;

/**
 * A consecutive block of memory, starting at a {@link MemoryLocation} with a fixed size.
 *
 */
public class MemoryBlock extends MemoryLocation {

  /** Special `pageNumber` value for pages which were not allocated by TaskMemoryManagers */
  public static final int NO_PAGE_NUMBER = -1;

  /**
   * Special `pageNumber` value for marking pages that have been freed in the TaskMemoryManager.
   * We set `pageNumber` to this value in TaskMemoryManager.freePage() so that MemoryAllocator
   * can detect if pages which were allocated by TaskMemoryManager have been freed in the TMM
   * before being passed to MemoryAllocator.free() (it is an error to allocate a page in
   * TaskMemoryManager and then directly free it in a MemoryAllocator without going through
   * the TMM freePage() call).
   */
  public static final int FREED_IN_TMM_PAGE_NUMBER = -2;

  /**
   * Special `pageNumber` value for pages that have been freed by the MemoryAllocator. This allows
   * us to detect double-frees.
   */
  public static final int FREED_IN_ALLOCATOR_PAGE_NUMBER = -3;
    /**
     * 当前MemoryBlock的连续内存块的长度
     */
  private final long length;

  /**
   * 当前MemoryBlock的页号。TaskMemoryManager分配由MemoryBlock表示的Page时，将使用此属性
   *
   * Optional page number; used when this MemoryBlock represents a page allocated by a
   * TaskMemoryManager. This field is public so that it can be modified by the TaskMemoryManager,
   * which lives in a different package.
   */
  public int pageNumber = NO_PAGE_NUMBER;

    /**
     * @param obj  我们要存储的对象
     * @param offset
     * @param length
     */
  public MemoryBlock(@Nullable Object obj, long offset, long length) {
    super(obj, offset);
    this.length = length;
  }

  /**
   * Returns the size of the memory block.
   */
  public long size() {
    return length;
  }

  /**
   * 创建一个指向由长整型数组使用的内存的MemoryBlock
   */
  public static MemoryBlock fromLongArray(final long[] array) {
    return new MemoryBlock(array, Platform.LONG_ARRAY_OFFSET, array.length * 8L);
  }

  /**
   * 以指定的字节填充整个Memory Block，即将obj对象从offset开始，长度为
   * length的堆内存替换为指定字节的值。Platform中封装了对sun.misc.Unsafe的API调用，
   * Platform的set Memory方法实际调用了sun.misc.Unsafe的set Memory方法。
   */
  public void fill(byte value) {
    Platform.setMemory(obj, offset, length, value);
  }
}
