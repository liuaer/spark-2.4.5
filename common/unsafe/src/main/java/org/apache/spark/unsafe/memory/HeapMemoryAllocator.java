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

import javax.annotation.concurrent.GuardedBy;
import java.lang.ref.WeakReference;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

import org.apache.spark.unsafe.Platform;

/**
 *
 * 文档：HeapMemoryAllocator 工作原理.note
 * 链接：http://note.youdao.com/noteshare?id=02902ff631bd8863d055bf8dc0bd6aca&sub=D5BC981C06C6448CB2CD867FFC2A7761
 * A simple {@link MemoryAllocator} that can allocate up to 16GB using a JVM long primitive array.
 */
public class HeapMemoryAllocator implements MemoryAllocator {

    /**
     *  MemoryBlock的弱引用的缓冲池，即Page页的分配
     *
     *  Map<分配内存的大小值, LinkedList<MemoryBlock>>>
     *
     *  // As an additional layer of defense against use-after-free bugs, we mutate the
     *  // MemoryBlock to null out its reference to the long[] array.
     *  long[] array = (long[]) memory.obj;
     *
     *  针对2.11 做了改动
     */    @GuardedBy("this")
  private final Map<Long, LinkedList<WeakReference<long[]>>> bufferPoolsBySize = new HashMap<>();

  private static final int POOLING_THRESHOLD_BYTES = 1024 * 1024;

  /**
   * Returns true if allocations of the given size should go through the pooling mechanism and
   * false otherwise.
   */
  private boolean shouldPool(long size) {
    // Very small allocations are less likely to benefit from pooling.
    return size >= POOLING_THRESHOLD_BYTES;
  }


  @Override
  public MemoryBlock allocate(long size) throws OutOfMemoryError {
      // 这行在干嘛？将size转换成8的整数倍,算是优化
    int numWords = (int) ((size + 7) / 8);
    long alignedSize = numWords * 8L;
    assert (alignedSize >= size);
    // 是否需要池化 ，Very small allocations are less likely to benefit from pooling.
    if (shouldPool(alignedSize)) {
      synchronized (this) {
          // 从池子中获取 alignedSize 大小的 pool
        final LinkedList<WeakReference<long[]>> pool = bufferPoolsBySize.get(alignedSize);
        if (pool != null) {
          while (!pool.isEmpty()) {
            final WeakReference<long[]> arrayReference = pool.pop();
            final long[] array = arrayReference.get();
            if (array != null) {
              assert (array.length * 8L >= size);
              MemoryBlock memory = new MemoryBlock(array, Platform.LONG_ARRAY_OFFSET, size);
                /*
                   是否分别用0xa5和0x5a字节来填充新分配的内存和释放内存。
                   这有助于捕获未初始化或已释放内存的滥用，但会增加一些开销。
                 */
              if (MemoryAllocator.MEMORY_DEBUG_FILL_ENABLED) {
                  // 用0xa5来填充新分配的内存
                memory.fill(MemoryAllocator.MEMORY_DEBUG_FILL_CLEAN_VALUE);
              }
              return memory;
            }
          }
          bufferPoolsBySize.remove(alignedSize);
        }
      }
      /*
          分配的内存是从 public void free(MemoryBlock memory) 中得到的
       */
    }
    /*
        不需要池化，直接分配 一个 MemoryBlock
     */
    long[] array = new long[numWords];
    MemoryBlock memory = new MemoryBlock(array, Platform.LONG_ARRAY_OFFSET, size);
    if (MemoryAllocator.MEMORY_DEBUG_FILL_ENABLED) {
      memory.fill(MemoryAllocator.MEMORY_DEBUG_FILL_CLEAN_VALUE);
    }
    return memory;
  }

  @Override
  public void free(MemoryBlock memory) {
    assert (memory.obj != null) :
      "baseObject was null; are you trying to use the on-heap allocator to free off-heap memory?";
    assert (memory.pageNumber != MemoryBlock.FREED_IN_ALLOCATOR_PAGE_NUMBER) :
      "page has already been freed";
    assert ((memory.pageNumber == MemoryBlock.NO_PAGE_NUMBER)
            || (memory.pageNumber == MemoryBlock.FREED_IN_TMM_PAGE_NUMBER)) :
      "TMM-allocated pages must first be freed via TMM.freePage(), not directly in allocator " +
        "free()";

    final long size = memory.size();
    // 0x5a 填充释放的内存。
    if (MemoryAllocator.MEMORY_DEBUG_FILL_ENABLED) {
      memory.fill(MemoryAllocator.MEMORY_DEBUG_FILL_FREED_VALUE);
    }

    // Mark the page as freed (so we can detect double-frees).
      // 将该 MemoryBlock 标记为 free
    memory.pageNumber = MemoryBlock.FREED_IN_ALLOCATOR_PAGE_NUMBER;

    // As an additional layer of defense against use-after-free bugs, we mutate the
    // MemoryBlock to null out its reference to the long[] array.
      // 将 MemoryBlock 转换成 long[]
    long[] array = (long[]) memory.obj;
    // 将 MemoryBlock 内容清空
    memory.setObjAndOffset(null, 0);

    // 将释放的大小放置到  bufferPoolsBySize
    long alignedSize = ((size + 7) / 8) * 8;
    if (shouldPool(alignedSize)) {
      synchronized (this) {
        LinkedList<WeakReference<long[]>> pool = bufferPoolsBySize.get(alignedSize);
        if (pool == null) {
          pool = new LinkedList<>();
          bufferPoolsBySize.put(alignedSize, pool);
        }
        pool.add(new WeakReference<>(array));
      }
    } else {
      // Do nothing
    }
  }
}
