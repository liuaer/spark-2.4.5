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

import java.io.{File, IOException}
import java.util.UUID

import org.apache.spark.SparkConf
import org.apache.spark.executor.ExecutorExitCode
import org.apache.spark.internal.Logging
import org.apache.spark.util.{ShutdownHookManager, Utils}
/**
  *
  * 负责为逻辑的block与磁盘块之间建立映射关系<br>
  * 一个块映射到一个文件，该文件的名称由它的BlockId提供<br>
  *  文件结构目录：
  *
  *  localDirs     Blockmgr-UUID-0  Blockmgr-UUID-1  <br><br>
  *

  *  subDirs    0 1  .... N           0  1 .....N  <br><br>
  *             |                       | \
  *             |                       |  \
  *  blocks     blockA             blockC   blockD<br>
  *
    图中显示了一级目录,即 localIs数组。图中的一级目录名称都采用了简单的
    示意表示,例如, Blockmgr-UUID-0、 Blockmgr-UUID-1、 Blockmgr-UUID-2,代表每个
    文件夹名称由 Blockmgr-和UUD生成的随机串组成,且此随机串不相同。 subDirs表示
    DiskBlockManager管理的二级目录,每个一级目录下都有N个二级目录,这里使用N代表
    subDirs perlocaldir属性的大小。每个二级目录下有若干个 Block的文件,有些二级目录下
    可能暂时还没有 Block文件。有了对 DiskBlockManager管理的文件目录结构的整体认识,
    下面来看看 DiskBlockManager提供了哪些方法来构成和管理这些目录。
 *
 * Block files are hashed among the directories listed in spark.local.dir (or in
 * SPARK_LOCAL_DIRS, if it's set).
 */
private[spark] class DiskBlockManager(conf: SparkConf, deleteFilesOnStop: Boolean) extends Logging {
  /**
    * 磁盘存储 DiskStore的本地子目录的数量。可以通过spark.diskStore.subDirectories设置
    * subDirectories属性配置,默认为64。
    */
  private[spark] val subDirsPerLocalDir = conf.getInt("spark.diskStore.subDirectories", 64)

  /* Create one local directory for each path mentioned in spark.local.dir; then, inside this
   * directory, create multiple subdirectories that we will hash files into, in order to avoid
   * having really large inodes at the top level. */
  /**
    localIs是 DiskBlockManager管理的本地目录数组。 localIs是通过调用 createLocalDirs创建的本地目录数组,
    其实质是调用了Uil工具类的getConfiguredocalDirs方法获取本地路径(getConfiguredLocalDirs方
    法默认获取 spark.local. dir属性或者系统属性 Java.io.tmpdir指定的目录,目录可能有多个),
    并在每个路径下创建以blackmer-为前缀,UUID为后缀的随机字符串的子目录,例如:
    [[\blockmgr-4949e19c-490c-48fc-ad6a-d80f4dbe73df]].
   */
  private[spark] val localDirs: Array[File] = createLocalDirs(conf)
  if (localDirs.isEmpty) {
    logError("Failed to create any local dir.")
    System.exit(ExecutorExitCode.DISK_STORE_FAILED_TO_CREATE_DIR)
  }

  /**
    *  DiskStore的本地子目录的二维数组
    *  The content of subDirs is  [[immutable]] but the content of subDirs(i) is [[mutable]]
    *  And the content of subDirs(i) is protected by the lock of subDirs(i)
    */
  private val subDirs = Array.fill(localDirs.length)(new Array[File](subDirsPerLocalDir))

  private val shutdownHook = addShutdownHook()

  /**

    *
    * */
  // This method should be kept in sync with
  // org.apache.spark.network.shuffle.ExternalShuffleBlockResolver#getFile().
  /*
    * *  文件结构目录：
    * *
    * *  localDirs   Blockmgr-UUID-0  Blockmgr-UUID-1  <br><br>
    * *
    * *
    * *  subDirs    0 1  .... N           0  1 .....N  <br><br>
    * *             |                       | \
    * *             |                       |  \
    * *  blocks     blockA             blockC   blockD<br>
   */
  def getFile(filename: String): File = {
    // Figure out which local directory it hashes to, and which subdirectory in that
    val hash = Utils.nonNegativeHash(filename)
    //从 localDirs 数组中按照取余方式获得选中的一级目录。
    val dirId = hash % localDirs.length
    // 哈希值除以一级目录的大小获得商,然后用商数与 subDirs Perlocaldir取余获得的余数作为选中的二级目录。
    val subDirId = (hash / localDirs.length) % subDirsPerLocalDir

    // 获取二级目录。如果二级目录不存在,则需要创建二级目录。
    val subDir = subDirs(dirId).synchronized {
      val old = subDirs(dirId)(subDirId)
      if (old != null) {
        old
      } else {
        // 创建了一个新的目录
        val newDir = new File(localDirs(dirId), "%02x".format(subDirId))
        if (!newDir.exists() && !newDir.mkdir()) {
          throw new IOException(s"Failed to create local dir in $newDir.")
        }
        subDirs(dirId)(subDirId) = newDir
        newDir
      }
    }

    new File(subDir, filename)
  }

  def getFile(blockId: BlockId): File = getFile(blockId.name)

  /** Check if disk block manager has a block. */
  def containsBlock(blockId: BlockId): Boolean = {
    getFile(blockId.name).exists()
  }

  /** List all the files currently stored on disk by the disk manager. */
  def getAllFiles(): Seq[File] = {
    // Get all the files inside the array of array of directories
    subDirs.flatMap { dir =>
      dir.synchronized {
        // Copy the content of dir because it may be modified in other threads
        dir.clone()
      }
    }.filter(_ != null).flatMap { dir =>
      val files = dir.listFiles()
      if (files != null) files else Seq.empty
    }
  }

  /** List all the blocks currently stored on disk by the disk manager. */
  def getAllBlocks(): Seq[BlockId] = {
    getAllFiles().flatMap { f =>
      try {
        Some(BlockId(f.getName))
      } catch {
        case _: UnrecognizedBlockId =>
          // Skip files which do not correspond to blocks, for example temporary
          // files created by [[SortShuffleWriter]].
          None
      }
    }
  }

  /** Produces a unique block id and File suitable for storing local intermediate results. */
  def createTempLocalBlock(): (TempLocalBlockId, File) = {
    var blockId = new TempLocalBlockId(UUID.randomUUID())
    while (getFile(blockId).exists()) {
      blockId = new TempLocalBlockId(UUID.randomUUID())
    }
    (blockId, getFile(blockId))
  }

  /** Produces a unique block id and File suitable for storing shuffled intermediate results. */
  def createTempShuffleBlock(): (TempShuffleBlockId, File) = {
    var blockId = new TempShuffleBlockId(UUID.randomUUID())
    while (getFile(blockId).exists()) {
      blockId = new TempShuffleBlockId(UUID.randomUUID())
    }
    (blockId, getFile(blockId))
  }

  /**
   * Create local directories for storing block data. These directories are
   * located inside configured local directories and won't
   * be deleted on JVM exit when using the external shuffle service.
   */
  private def createLocalDirs(conf: SparkConf): Array[File] = {
    Utils.getConfiguredLocalDirs(conf).flatMap { rootDir =>
      try {
        val localDir = Utils.createDirectory(rootDir, "blockmgr")
        logInfo(s"Created local directory at $localDir")
        Some(localDir)
      } catch {
        case e: IOException =>
          logError(s"Failed to create local dir in $rootDir. Ignoring this directory.", e)
          None
      }
    }
  }

  private def addShutdownHook(): AnyRef = {
    logDebug("Adding shutdown hook") // force eager creation of logger
    ShutdownHookManager.addShutdownHook(ShutdownHookManager.TEMP_DIR_SHUTDOWN_PRIORITY + 1) { () =>
      logInfo("Shutdown hook called")
      DiskBlockManager.this.doStop()
    }
  }

  /** Cleanup local dirs and stop shuffle sender. */
  private[spark] def stop() {
    // Remove the shutdown hook.  It causes memory leaks if we leave it around.
    try {
      ShutdownHookManager.removeShutdownHook(shutdownHook)
    } catch {
      case e: Exception =>
        logError(s"Exception while removing shutdown hook.", e)
    }
    doStop()
  }

  private def doStop(): Unit = {
    if (deleteFilesOnStop) {
      localDirs.foreach { localDir =>
        if (localDir.isDirectory() && localDir.exists()) {
          try {
            if (!ShutdownHookManager.hasRootAsShutdownDeleteDir(localDir)) {
              Utils.deleteRecursively(localDir)
            }
          } catch {
            case e: Exception =>
              logError(s"Exception while deleting local spark dir: $localDir", e)
          }
        }
      }
    }
  }
}
