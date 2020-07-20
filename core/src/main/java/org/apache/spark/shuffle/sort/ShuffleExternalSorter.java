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

package org.apache.spark.shuffle.sort;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.util.LinkedList;

import scala.Tuple2;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.spark.SparkConf;
import org.apache.spark.TaskContext;
import org.apache.spark.executor.ShuffleWriteMetrics;
import org.apache.spark.internal.config.package$;
import org.apache.spark.memory.MemoryConsumer;
import org.apache.spark.memory.SparkOutOfMemoryError;
import org.apache.spark.memory.TaskMemoryManager;
import org.apache.spark.memory.TooLargePageException;
import org.apache.spark.serializer.DummySerializerInstance;
import org.apache.spark.serializer.SerializerInstance;
import org.apache.spark.storage.BlockManager;
import org.apache.spark.storage.DiskBlockObjectWriter;
import org.apache.spark.storage.FileSegment;
import org.apache.spark.storage.TempShuffleBlockId;
import org.apache.spark.unsafe.Platform;
import org.apache.spark.unsafe.UnsafeAlignedOffset;
import org.apache.spark.unsafe.array.LongArray;
import org.apache.spark.unsafe.memory.MemoryBlock;
import org.apache.spark.util.Utils;

/**
 *
 * Shuffle ExternalSorter是专门用于对 Shuffle 数据进行排序的外部排序器， 用于将 map
 * 任务的输出存储到 Tungsten 中； 在记录超过限制时，将数据溢出到磁盘。与 ExternalSorter
 * 不同， ShuffleExternalSorter 本身并没有实现数据的持久化功能，具体的持久化将由
 * ShuffleExternalSorter 的调用者 UnsafeShuffleWriter来实现。
 *
 * <br>
 * An external sorter that is specialized for sort-based shuffle.
 * <p>
 * Incoming records are appended to data pages. When all records have been inserted (or when the
 * current thread's shuffle memory limit is reached), the in-memory records are sorted according to
 * their partition ids (using a {@link ShuffleInMemorySorter}). The sorted records are then
 * written to a single output file (or multiple files, if we've spilled). The format of the output
 * files is the same as the format of the final output file written by
 * {@link org.apache.spark.shuffle.sort.SortShuffleWriter}: each output partition's records are
 * written as a single serialized, compressed stream that can be read with a new decompression and
 * deserialization stream.
 * <p>
 * Unlike {@link org.apache.spark.util.collection.ExternalSorter}, this sorter does not merge its
 * spill files. Instead, this merging is performed in {@link UnsafeShuffleWriter}, which uses a
 * specialized merge procedure that avoids extra serialization/deserialization.
 */
final class ShuffleExternalSorter extends MemoryConsumer {

  private static final Logger logger = LoggerFactory.getLogger(ShuffleExternalSorter.class);

  @VisibleForTesting
  static final int DISK_WRITE_BUFFER_SIZE = 1024 * 1024;

  private final int numPartitions;
  private final TaskMemoryManager taskMemoryManager; // 对每一个Task的内存分配释放进行管理
    /*
        BlockManager运行在每个节点上(包括 Driver和 Executor),提供对本地或远端节点上
     *  的内存、磁盘及堆外内存中 Block 的管理
     */
  private final BlockManager blockManager;
  private final TaskContext taskContext;
  private final ShuffleWriteMetrics writeMetrics;

  /**
   * Force this sorter to spill when there are this many elements in memory.
   */
  private final int numElementsForSpillThreshold;

  /**
   * 创建的 DiskBlockObject Writer内部的文件缓冲大小。可通过
   * spark.shuffle.file.buffer属性进行配置,默认是 32KB
   */
  private final int fileBufferSizeBytes;

  /** The buffer size to use when writing the sorted records to an on-disk file */
  private final int diskWriteBufferSize;

  /**
   *  已经分配的 Page 列表
   *
   * Memory pages that hold the records being sorted. The pages in this list are freed when
   * spilling, although in principle we could recycle these pages across spills (on the other hand,
   * this might not be necessary if we maintained a pool of re-usable pages in the TaskMemoryManager
   * itself).
   */
  private final LinkedList<MemoryBlock> allocatedPages = new LinkedList<>();
    /**
     *  溢出文件的元数据信息列表
     */
  private final LinkedList<SpillInfo> spills = new LinkedList<>();

  /** 内存中数据结构的峰值 **/
  private long peakMemoryUsedBytes;

    /**
     *  用于在内存中对插入的数据进行排序
     */
  @Nullable private ShuffleInMemorySorter inMemSorter;
  @Nullable private MemoryBlock currentPage = null;
    /**
     * Page的光标。实际为用于向 Tungsten写入数据时的地址信息
     */
  private long pageCursor = -1;

  ShuffleExternalSorter(
      TaskMemoryManager memoryManager,
      BlockManager blockManager,
      TaskContext taskContext,
      int initialSize,
      int numPartitions,
      SparkConf conf,
      ShuffleWriteMetrics writeMetrics) {
    super(memoryManager,
      (int) Math.min(PackedRecordPointer.MAXIMUM_PAGE_SIZE_BYTES, memoryManager.pageSizeBytes()),
      memoryManager.getTungstenMemoryMode());
    this.taskMemoryManager = memoryManager;
    this.blockManager = blockManager;
    this.taskContext = taskContext;
    this.numPartitions = numPartitions;
    // Use getSizeAsKb (not bytes) to maintain backwards compatibility if no units are provided
    this.fileBufferSizeBytes =
        (int) (long) conf.get(package$.MODULE$.SHUFFLE_FILE_BUFFER_SIZE()) * 1024;
    this.numElementsForSpillThreshold =
        (int) conf.get(package$.MODULE$.SHUFFLE_SPILL_NUM_ELEMENTS_FORCE_SPILL_THRESHOLD());
    this.writeMetrics = writeMetrics;
    this.inMemSorter = new ShuffleInMemorySorter(
      this, initialSize, conf.getBoolean("spark.shuffle.sort.useRadixSort", true));
    this.peakMemoryUsedBytes = getMemoryUsage();
    this.diskWriteBufferSize =
        (int) (long) conf.get(package$.MODULE$.SHUFFLE_DISK_WRITE_BUFFER_SIZE());
  }

  /**
   * Sorts the in-memory records and writes the sorted records to an on-disk file.
   * This method does not free the sort data structures.
   *
   * @param isLastFile if true, this indicates that we're writing the final output file and that the
   *                   bytes written should be counted towards shuffle spill metrics rather than
   *                   shuffle write metrics.
   */
  private void writeSortedFile(boolean isLastFile) {

    final ShuffleWriteMetrics writeMetricsToUse;

    if (isLastFile) {
      // We're writing the final non-spill file, so we _do_ want to count this as shuffle bytes.
      writeMetricsToUse = writeMetrics;
    } else {
      // We're spilling, so bytes written should be counted towards spill rather than write.
      // Create a dummy WriteMetrics object to absorb these metrics, since we don't want to count
      // them towards shuffle bytes written.
      writeMetricsToUse = new ShuffleWriteMetrics();
    }

    // This call performs the actual sort.
    final ShuffleInMemorySorter.ShuffleSorterIterator sortedRecords =
      inMemSorter.getSortedIterator();

    // Small writes to DiskBlockObjectWriter will be fairly inefficient. Since there doesn't seem to
    // be an API to directly transfer bytes from managed memory to the disk writer, we buffer
    // data through a byte array. This array does not need to be large enough to hold a single
    // record;
    final byte[] writeBuffer = new byte[diskWriteBufferSize];

    // Because this output will be read during shuffle, its compression codec must be controlled by
    // spark.shuffle.compress instead of spark.shuffle.spill.compress, so we need to use
    // createTempShuffleBlock here; see SPARK-3426 for more details.
    final Tuple2<TempShuffleBlockId, File> spilledFileInfo =
      blockManager.diskBlockManager().createTempShuffleBlock();
    final File file = spilledFileInfo._2();
    final TempShuffleBlockId blockId = spilledFileInfo._1();
    final SpillInfo spillInfo = new SpillInfo(numPartitions, file, blockId);

    // Unfortunately, we need a serializer instance in order to construct a DiskBlockObjectWriter.
    // Our write path doesn't actually use this serializer (since we end up calling the `write()`
    // OutputStream methods), but DiskBlockObjectWriter still calls some methods on it. To work
    // around this, we pass a dummy no-op serializer.
    final SerializerInstance ser = DummySerializerInstance.INSTANCE;

    final DiskBlockObjectWriter writer =
      blockManager.getDiskWriter(blockId, file, ser, fileBufferSizeBytes, writeMetricsToUse);

    int currentPartition = -1;
    final int uaoSize = UnsafeAlignedOffset.getUaoSize();
    while (sortedRecords.hasNext()) {
      sortedRecords.loadNext();
      final int partition = sortedRecords.packedRecordPointer.getPartitionId();
      assert (partition >= currentPartition);
      if (partition != currentPartition) {
        // Switch to the new partition
        if (currentPartition != -1) {
          final FileSegment fileSegment = writer.commitAndGet();
          spillInfo.partitionLengths[currentPartition] = fileSegment.length();
        }
        currentPartition = partition;
      }

      final long recordPointer = sortedRecords.packedRecordPointer.getRecordPointer();
      final Object recordPage = taskMemoryManager.getPage(recordPointer);
      final long recordOffsetInPage = taskMemoryManager.getOffsetInPage(recordPointer);
      int dataRemaining = UnsafeAlignedOffset.getSize(recordPage, recordOffsetInPage);
      long recordReadPosition = recordOffsetInPage + uaoSize; // skip over record length
      while (dataRemaining > 0) {
        final int toTransfer = Math.min(diskWriteBufferSize, dataRemaining);
        Platform.copyMemory(
          recordPage, recordReadPosition, writeBuffer, Platform.BYTE_ARRAY_OFFSET, toTransfer);
        writer.write(writeBuffer, 0, toTransfer);
        recordReadPosition += toTransfer;
        dataRemaining -= toTransfer;
      }
      writer.recordWritten();
    }

    final FileSegment committedSegment = writer.commitAndGet();
    writer.close();
    // If `writeSortedFile()` was called from `closeAndGetSpills()` and no records were inserted,
    // then the file might be empty. Note that it might be better to avoid calling
    // writeSortedFile() in that case.
    if (currentPartition != -1) {
      spillInfo.partitionLengths[currentPartition] = committedSegment.length();
      spills.add(spillInfo);
    }

    if (!isLastFile) {  // i.e. this is a spill file
      // The current semantics of `shuffleRecordsWritten` seem to be that it's updated when records
      // are written to disk, not when they enter the shuffle sorting code. DiskBlockObjectWriter
      // relies on its `recordWritten()` method being called in order to trigger periodic updates to
      // `shuffleBytesWritten`. If we were to remove the `recordWritten()` call and increment that
      // counter at a higher-level, then the in-progress metrics for records written and bytes
      // written would get out of sync.
      //
      // When writing the last file, we pass `writeMetrics` directly to the DiskBlockObjectWriter;
      // in all other cases, we pass in a dummy write metrics to capture metrics, then copy those
      // metrics to the true write metrics here. The reason for performing this copying is so that
      // we can avoid reporting spilled bytes as shuffle write bytes.
      //
      // Note that we intentionally ignore the value of `writeMetricsToUse.shuffleWriteTime()`.
      // Consistent with ExternalSorter, we do not count this IO towards shuffle write time.
      // This means that this IO time is not accounted for anywhere; SPARK-3577 will fix this.
      writeMetrics.incRecordsWritten(writeMetricsToUse.recordsWritten());
      taskContext.taskMetrics().incDiskBytesSpilled(writeMetricsToUse.bytesWritten());
    }
  }

  /**
   * Sort and spill the current records in response to memory pressure.
   */
  @Override
  public long spill(long size, MemoryConsumer trigger) throws IOException {
    if (trigger != this || inMemSorter == null || inMemSorter.numRecords() == 0) {
      return 0L;
    }

    logger.info("Thread {} spilling sort data of {} to disk ({} {} so far)",
      Thread.currentThread().getId(),
      Utils.bytesToString(getMemoryUsage()),
      spills.size(),
      spills.size() > 1 ? " times" : " time");

    writeSortedFile(false);
    final long spillSize = freeMemory();
    inMemSorter.reset();
    // Reset the in-memory sorter's pointer array only after freeing up the memory pages holding the
    // records. Otherwise, if the task is over allocated memory, then without freeing the memory
    // pages, we might not be able to get memory for the pointer array.
    taskContext.taskMetrics().incMemoryBytesSpilled(spillSize);
    return spillSize;
  }

  private long getMemoryUsage() {
    long totalPageSize = 0;
    for (MemoryBlock page : allocatedPages) {
      totalPageSize += page.size();
    }
    return ((inMemSorter == null) ? 0 : inMemSorter.getMemoryUsage()) + totalPageSize;
  }

  private void updatePeakMemoryUsed() {
    long mem = getMemoryUsage();
    if (mem > peakMemoryUsedBytes) {
      peakMemoryUsedBytes = mem;
    }
  }

  /**
   * Return the peak memory used so far, in bytes.
   */
  long getPeakMemoryUsedBytes() {
    updatePeakMemoryUsed();
    return peakMemoryUsedBytes;
  }

  private long freeMemory() {
    updatePeakMemoryUsed();
    long memoryFreed = 0;
    for (MemoryBlock block : allocatedPages) {
      memoryFreed += block.size();
      freePage(block);
    }
    allocatedPages.clear();
    currentPage = null;
    pageCursor = 0;
    return memoryFreed;
  }

  /**
   * Force all memory and spill files to be deleted; called by shuffle error-handling code.
   */
  public void cleanupResources() {
    freeMemory();
    if (inMemSorter != null) {
      inMemSorter.free();
      inMemSorter = null;
    }
    for (SpillInfo spill : spills) {
      if (spill.file.exists() && !spill.file.delete()) {
        logger.error("Unable to delete spill file {}", spill.file.getPath());
      }
    }
  }

  /**
   *
   * 检查是否有足够的空间将额外的记录插入到排序指针数组中,如果需要额外的空间,则增加数组的容量。
   * 如果无法获取所需的空间,则内存中的数据将被溢出到磁盘。
   *
   *
   * Checks whether there is enough space to insert an additional record in to the sort pointer
   * array and grows the array if additional space is required. If the required space cannot be
   * obtained, then the in-memory data will be spilled to disk.
   */
  private void growPointerArrayIfNecessary() throws IOException {
    assert(inMemSorter != null);
    // 没有 足够的空间将额外的记录插入到排序指针数组中
    if (!inMemSorter.hasSpaceForAnotherRecord()) {
        // 获取已经使用的内存(字节数)
      long used = inMemSorter.getMemoryUsage();
      LongArray array;
      try {
        // could trigger spilling
          // used / 8 是为了转换成 Long , *2 是为了给数组扩容
        array = allocateArray(used / 8 * 2);
      } catch (TooLargePageException e) {
        // The pointer array is too big to fix in a single page, spill.
          // 数组太大无法申请
        spill();
        return;
      } catch (SparkOutOfMemoryError e) {
        // should have trigger spilling
        if (!inMemSorter.hasSpaceForAnotherRecord()) {
          logger.error("Unable to grow the pointer array");
          throw e;
        }
        return;
      }
      // check if spilling is triggered or not
        // 已经溢出过了，则将内存中释放掉
      if (inMemSorter.hasSpaceForAnotherRecord()) {
        freeArray(array);
      } else {
          // 没有额外的空间
        inMemSorter.expandPointerArray(array);
      }
    }
  }

  /**
   * Allocates more memory in order to insert an additional record. This will request additional
   * memory from the memory manager and spill if the requested memory can not be obtained.
   *
   * @param required the required space in the data page, in bytes, including space for storing
   *                      the record size. This must be less than or equal to the page size (records
   *                      that exceed the page size are handled via a different code path which uses
   *                      special overflow pages).
   */
  private void acquireNewPageIfNecessary(int required) {
    if (currentPage == null ||
      pageCursor + required > currentPage.getBaseOffset() + currentPage.size() ) {
      // TODO: try to find space in previous pages
      currentPage = allocatePage(required);
      pageCursor = currentPage.getBaseOffset();
      allocatedPages.add(currentPage);
    }
  }

  /**
   * map任务在执行结束后会将数据写人磁盘， 等待reduce任务获取。但在写入磁盘之前，
   * Spark可能会对map任务的输出在内存中进行一些排序和聚合。ShuffleExternalSorter的
   * insertRecord 方法是这一过程的入口
   */
  public void insertRecord(Object recordBase, long recordOffset, int length, int partitionId)
    throws IOException {

    // 如果 ShuffleInMemorySorter 中的记录数大于等于 numElementsForSpillThreshold
    assert(inMemSorter != null);
    if (inMemSorter.numRecords() >= numElementsForSpillThreshold) {
      logger.info("Spilling data because number of spilledRecords crossed the threshold " +
        numElementsForSpillThreshold);
      // 溢出
      spill();
    }
    /*
       检查是否有足够的空间将额外的记录插入到排序指针数组中，如果需要额外的空间，则增加数组的容量。
       如果无法获取所需的空间,则将内存中的数据溢出到磁盘
     */
    growPointerArrayIfNecessary();
    final int uaoSize = UnsafeAlignedOffset.getUaoSize();
    // Need 4 or 8 bytes to store the record length.
    final int required = length + uaoSize;
    /*
      检查是否有足够的空间(数据长度和4之和这4字节用于在内存中存储数据的长度),
      如果需要额外的空间,则申请分配新的Page
     */
    acquireNewPageIfNecessary(required);
    assert(currentPage != null);
    final Object base = currentPage.getBaseObject();
    // 返回页号和相对于内存块起始地址的偏移量(64位长整型)。
    final long recordAddress = taskMemoryManager.encodePageNumberAndOffset(currentPage, pageCursor);
    UnsafeAlignedOffset.putSize(base, pageCursor, length);
    pageCursor += uaoSize;
    Platform.copyMemory(recordBase, recordOffset, base, pageCursor, length);
    pageCursor += length;
    inMemSorter.insertRecord(recordAddress, partitionId);
  }

  /**
   * Close the sorter, causing any buffered data to be sorted and written out to disk.
   *
   * @return metadata for the spill files written by this sorter. If no records were ever inserted
   *         into this sorter, then this will return an empty array.
   * @throws IOException
   */
  public SpillInfo[] closeAndGetSpills() throws IOException {
    if (inMemSorter != null) {
      // Do not count the final file towards the spill count.
      writeSortedFile(true);
      freeMemory();
      inMemSorter.free();
      inMemSorter = null;
    }
    return spills.toArray(new SpillInfo[spills.size()]);
  }

}
