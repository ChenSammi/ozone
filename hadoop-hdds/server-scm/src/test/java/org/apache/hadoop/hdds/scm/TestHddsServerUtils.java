/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdds.scm;

import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_CLIENT_ADDRESS_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_DATANODE_ADDRESS_KEY;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_DATANODE_ID_DIR;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_DATANODE_PORT_DEFAULT;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_NAMES;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_STALENODE_INTERVAL;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_SCM_DATANODE_ID_FILE_DEFAULT;
import static org.apache.hadoop.ozone.container.metadata.DatanodeSchemaThreeDBDefinition.getContainerKeyPrefix;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.lang.management.OperatingSystemMXBean;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import com.sun.management.UnixOperatingSystemMXBean;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.DeletedBlocksTransaction;
import org.apache.hadoop.hdds.scm.ha.SCMNodeInfo;
import org.apache.hadoop.hdds.server.ServerUtils;
import org.apache.hadoop.hdds.utils.HddsServerUtil;
import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.hdds.utils.db.CodecException;
import org.apache.hadoop.hdds.utils.db.RocksDatabaseException;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.TableIterator;
import org.apache.hadoop.hdds.utils.db.managed.ManagedOptions;
import org.apache.hadoop.hdds.utils.db.managed.ManagedRocksDB;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.ozone.container.common.helpers.BlockData;
import org.apache.hadoop.ozone.container.common.helpers.ChunkInfo;
import org.apache.hadoop.ozone.container.common.helpers.ChunkInfoList;
import org.apache.hadoop.ozone.container.metadata.DatanodeStore;
import org.apache.hadoop.ozone.container.metadata.DatanodeStoreSchemaThreeImpl;
import org.apache.hadoop.ozone.container.metadata.DeleteTransactionStore;
import org.apache.hadoop.test.PathUtils;
import org.junit.jupiter.api.Test;
import org.rocksdb.RocksDBException;

/**
 * Unit tests for {@link HddsServerUtil}.
 */
public class TestHddsServerUtils {

  /**
   * Test getting OZONE_SCM_DATANODE_ADDRESS_KEY with port.
   */
  @Test
  @SuppressWarnings("StringSplitter")
  public void testGetDatanodeAddressWithPort() {
    final String scmHost = "host123:100";
    final OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(OZONE_SCM_DATANODE_ADDRESS_KEY, scmHost);
    final InetSocketAddress address =
        NetUtils.createSocketAddr(
            SCMNodeInfo.buildNodeInfo(conf).get(0).getScmDatanodeAddress());
    assertEquals(address.getHostName(), scmHost.split(":")[0]);
    assertEquals(address.getPort(), Integer.parseInt(scmHost.split(":")[1]));
  }

  /**
   * Test getting OZONE_SCM_DATANODE_ADDRESS_KEY without port.
   */
  @Test
  public void testGetDatanodeAddressWithoutPort() {
    final String scmHost = "host123";
    final OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(OZONE_SCM_DATANODE_ADDRESS_KEY, scmHost);
    final InetSocketAddress address =
        NetUtils.createSocketAddr(
            SCMNodeInfo.buildNodeInfo(conf).get(0).getScmDatanodeAddress());
    assertEquals(scmHost, address.getHostName());
    assertEquals(OZONE_SCM_DATANODE_PORT_DEFAULT, address.getPort());
  }

  /**
   * When OZONE_SCM_DATANODE_ADDRESS_KEY is undefined, test fallback to
   * OZONE_SCM_CLIENT_ADDRESS_KEY.
   */
  @Test
  public void testDatanodeAddressFallbackToClientNoPort() {
    final String scmHost = "host123";
    final OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(OZONE_SCM_CLIENT_ADDRESS_KEY, scmHost);
    final InetSocketAddress address =
        NetUtils.createSocketAddr(
            SCMNodeInfo.buildNodeInfo(conf).get(0).getScmDatanodeAddress());
    assertEquals(scmHost, address.getHostName());
    assertEquals(OZONE_SCM_DATANODE_PORT_DEFAULT, address.getPort());
  }

  /**
   * When OZONE_SCM_DATANODE_ADDRESS_KEY is undefined, test fallback to
   * OZONE_SCM_CLIENT_ADDRESS_KEY. Port number defined by
   * OZONE_SCM_CLIENT_ADDRESS_KEY should be ignored.
   */
  @Test
  @SuppressWarnings("StringSplitter")
  public void testDatanodeAddressFallbackToClientWithPort() {
    final String scmHost = "host123:100";
    final OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(OZONE_SCM_CLIENT_ADDRESS_KEY, scmHost);
    final InetSocketAddress address =
        NetUtils.createSocketAddr(
            SCMNodeInfo.buildNodeInfo(conf).get(0).getScmDatanodeAddress());
    assertEquals(address.getHostName(), scmHost.split(":")[0]);
    assertEquals(address.getPort(), OZONE_SCM_DATANODE_PORT_DEFAULT);
  }

  /**
   * When OZONE_SCM_DATANODE_ADDRESS_KEY and OZONE_SCM_CLIENT_ADDRESS_KEY
   * are undefined, test fallback to OZONE_SCM_NAMES.
   */
  @Test
  public void testDatanodeAddressFallbackToScmNamesNoPort() {
    final String scmHost = "host123";
    final OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(OZONE_SCM_NAMES, scmHost);
    final InetSocketAddress address = NetUtils.createSocketAddr(
        SCMNodeInfo.buildNodeInfo(conf).get(0).getScmDatanodeAddress());
    assertEquals(scmHost, address.getHostName());
    assertEquals(OZONE_SCM_DATANODE_PORT_DEFAULT, address.getPort());
  }

  /**
   * When OZONE_SCM_DATANODE_ADDRESS_KEY and OZONE_SCM_CLIENT_ADDRESS_KEY
   * are undefined, test fallback to OZONE_SCM_NAMES. Port number
   * defined by OZONE_SCM_NAMES should be ignored.
   */
  @Test
  @SuppressWarnings("StringSplitter")
  public void testDatanodeAddressFallbackToScmNamesWithPort() {
    final String scmHost = "host123:100";
    final OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(OZONE_SCM_NAMES, scmHost);
    final InetSocketAddress address =
        NetUtils.createSocketAddr(
            SCMNodeInfo.buildNodeInfo(conf).get(0).getScmDatanodeAddress());
    assertEquals(address.getHostName(), scmHost.split(":")[0]);
    assertEquals(OZONE_SCM_DATANODE_PORT_DEFAULT, address.getPort());
  }

  /**
   * Test {@link ServerUtils#getScmDbDir}.
   */
  @Test
  public void testGetScmDbDir() {
    final File testDir = PathUtils.getTestDir(TestHddsServerUtils.class);
    final File dbDir = new File(testDir, "scmDbDir");
    final File metaDir = new File(testDir, "metaDir");   // should be ignored.
    final OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(ScmConfigKeys.OZONE_SCM_DB_DIRS, dbDir.getPath());
    conf.set(HddsConfigKeys.OZONE_METADATA_DIRS, metaDir.getPath());

    try {
      assertEquals(dbDir, ServerUtils.getScmDbDir(conf));
      assertTrue(dbDir.exists());          // should have been created.
    } finally {
      FileUtils.deleteQuietly(dbDir);
    }
  }

  @Test
  public void testGCPressure() {
    final OzoneConfiguration conf = new OzoneConfiguration();
    String path = "/Volumes/DiskFullTest/ozone-dn/hdds/CID-b59af358-d167-4a7a-aa78-6090e624a64f/DS-663a9099-7381-4561-a66c-06d8de9e45a1/container.db";
    final Object lock = new Object();
    // conf.set(ScmConfigKeys.OZONE_SCM_DB_DIRS, "/Users/sammi/workspace/ozone-meta/scm.db.1/");
    // SCMDBDefinition scmdbDefinition = SCMDBDefinition.get();
    // DBStore store = DBStoreBuilder.createDBStore(conf, scmdbDefinition);
    // store.compactDB();
    // System.out.println("DB closed ? " + store.isClosed());
//      if (!store.isClosed()) {
//        store.getStore().flushDB();
//      }
//    } catch (Exception e) {
//      e.printStackTrace();
//    }

    Thread gcThread = new Thread(() -> gcSimulator(lock));
    Thread dbThread = new Thread(() -> {
      getHeapUsage();
      synchronized (lock) {
        try {
          lock.wait();
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      }

      ManagedOptions managedOptions = new ManagedOptions();
      managedOptions.setCreateIfMissing(false);

      try (ManagedRocksDB storeReadonly = ManagedRocksDB.openReadOnly(managedOptions, path)) {
        storeReadonly.get().maxMemCompactionLevel();
        System.out.println("DB is opened");
      } catch (RocksDBException e) {
        throw new RuntimeException(e);
      }

      try (DatanodeStore store = new DatanodeStoreSchemaThreeImpl(conf, path, false)) {
        if (!store.isClosed()) {
          // Delete all entries from DeleteTransactionTable using iterator
          Table<String, DeletedBlocksTransaction> deleteTxns =
              ((DeleteTransactionStore<String>) store).getDeleteTransactionTable();
          long deleteTxnCount = 0;

          try (BatchOperation batch = store.getBatchHandler().initBatchOperation()) {
            try (TableIterator<String, DeletedBlocksTransaction> iter = deleteTxns.valueIterator()) {
              while (iter.hasNext()) {
                final DeletedBlocksTransaction tx = iter.next();
                deleteTxns.deleteWithBatch(batch, getTxKey(tx.getContainerID(), tx.getTxID()));
                deleteTxnCount++;
              }
            } catch (IOException e) {
              System.err.println("Failed to clear DeleteTransactionTable");
              e.printStackTrace();
            } finally {
              store.getBatchHandler().commitBatchOperation(batch);
              System.out.println("Deleted " + deleteTxnCount + " entries from DeleteTransactionTable");
            }
          }

          // Delete all entries from BlockDataTable using iterator
          Table<String, BlockData> blockDataTable = store.getBlockDataTable();
          int blockDataCount = 0;
          try {
            List<Table.KeyValue<String, BlockData>> list =
                blockDataTable.getRangeKVs(null, Integer.MAX_VALUE, null);
            for (Table.KeyValue<String, BlockData> entry : list) {
              blockDataTable.delete(entry.getKey());
              blockDataCount++;
            }
          } catch (IOException e) {
            System.err.println("Failed to clear BlockDataTable");
            e.printStackTrace();
          } finally {
            System.out.println("Deleted " + blockDataCount + " entries from BlockDataTable");
          }

          // Add 10000 block deletion transactions
          System.out.println("Adding 10000 block deletion transactions...");
          for (long txnId = 1; txnId <= 10000; txnId++) {
            DeletedBlocksTransaction.Builder txnBuilder = DeletedBlocksTransaction.newBuilder();
            txnBuilder.setTxID(txnId);
            long containerID = txnId % 100 + 1;
            txnBuilder.setContainerID(containerID); // Distribute across 100 containers

            // Add 10 blocks to each transaction
            for (int blockId = 1; blockId <= 10; blockId++) {
              long localBlockId = (txnId * 10L) + blockId;
              txnBuilder.addLocalID(localBlockId);

              // Create BlockData with 10 chunks for each block
              BlockData blockData = new BlockData(new BlockID(containerID, localBlockId));
              for (int chunkId = 1; chunkId <= 10; chunkId++) {
                ChunkInfo chunkInfo = new ChunkInfo(
                    String.format("chunk-%d-%d-%d", txnId, blockId, chunkId),
                    chunkId * 1024 * 1024L, // offset
                    1024 * 1024L); // 1MB per chunk
                blockData.addChunk(chunkInfo.getProtoBufMessage());
              }

              // Store block data in the datanode store
              try {
                // Store block data in the datanode store
                store.getBlockDataTable().put(getBlockDataKey(containerID, localBlockId), blockData);
              } catch (IOException e) {
                System.err.println("Failed to add block data for txn " + txnId + ", block " + blockId);
                e.printStackTrace();
                //store.flushLog(true);
                // try read content in case of NoSpace
                List<Table.KeyValue<String, BlockData>> list =
                    blockDataTable.getRangeKVs(null, Integer.MAX_VALUE, null);
                int count = 0;
                for (Table.KeyValue<String, BlockData> entry : list) {
                  System.out.println("Block data: " + entry.getValue());
                  count++;
                }
                System.out.println("Read " + count + " entries from BlockDataTable after exception");
                break;
              }
            }

            // Store the deletion transaction
            try {
              ((DeleteTransactionStore) store).getDeleteTransactionTable().put(
                  getTxKey(containerID, txnId), txnBuilder.build());
            } catch (IOException e) {
              System.err.println("Failed to add deletion transaction " + txnId);
              e.printStackTrace();
              break;
            }
          }
          System.out.println("Finished adding 10000 block deletion transactions");

          int count = 100;
          while (count > 0) {
            getHeapUsage();
            long startTime = System.currentTimeMillis();
            store.getStore().compactDB();
            System.out.println("DB compaction finished after " + (System.currentTimeMillis() - startTime) + "ms");
            count--;
          }
        }
      } catch (Exception e) {
        System.out.println("Exception in DB thread");
        e.printStackTrace();}
    });

    gcThread.start();
    dbThread.start();

    try {
      gcThread.join();
      dbThread.join();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  private String getTxKey(long containerID, Long txID) {
    return getContainerKeyPrefix(containerID) + Long.toString(txID);
  }

  private String getBlockDataKey(long containerID, Long localID) {
    return getContainerKeyPrefix(containerID) + Long.toString(localID);
  }

  @Test
  public void testTooManyOpenFiles() throws RocksDBException {
    final OzoneConfiguration conf = new OzoneConfiguration();
    String path = "/Volumes/DiskFullTest/ozone-dn/hdds/CID-b59af358-d167-4a7a-aa78-6090e624a64f/DS-663a9099-7381-4561-a66c-06d8de9e45a1/container.db";

    // Open files under /Volumes/DiskFullTest directory with names starting with "file-"
    File testDir = new File("/Volumes/DiskFullTest");
    List<FileInputStream> openFiles = new ArrayList<>();
    int filesOpened = 0;
    int maxFiles = 10011; // 10011 will fail normal open, 10012 will fail both opens

    if (testDir.exists() && testDir.isDirectory()) {
      File[] files = testDir.listFiles((dir, name) -> name.startsWith("file_8"));
      if (files != null) {
        for (File file : files) {
          if (filesOpened >= maxFiles) {
            break;
          }
          if (file.isFile()) {
            try {
              openFiles.add(new FileInputStream(file));
              filesOpened++;
            } catch (IOException e) {
              System.err.println("Failed to open file: " + file.getName());
            }
          }
        }
      }
    }
    System.out.println("Opened " + filesOpened + " files");

    OperatingSystemMXBean os = ManagementFactory.getOperatingSystemMXBean();
    if (os instanceof UnixOperatingSystemMXBean) {
      System.out.println("Max File Descriptors: " + ((UnixOperatingSystemMXBean) os).getMaxFileDescriptorCount());
      long openCount = ((UnixOperatingSystemMXBean) os).getOpenFileDescriptorCount();
      System.out.println("Open files: " + openCount);
    }

    // read-only mode
    ManagedOptions managedOptions = new ManagedOptions();
    managedOptions.setCreateIfMissing(false);
    ManagedRocksDB storeReadonly = ManagedRocksDB.openReadOnly(managedOptions, path);
    storeReadonly.get().maxMemCompactionLevel();
    System.out.println("DB is opened");

    // normal mode
    try (DatanodeStore store = new DatanodeStoreSchemaThreeImpl(conf, path, false)) {
      if (store.isClosed()) {
        System.out.println("DB is closed");
      }
    } catch (Exception e) {
      System.out.println("Failed to open DB");
      e.printStackTrace();
    }
  }

  @Test
  public void testCloseTwice() throws RocksDBException {
    final OzoneConfiguration conf = new OzoneConfiguration();
    String path = "/Volumes/DiskFullTest/ozone-dn/hdds/CID-b59af358-d167-4a7a-aa78-6090e624a64f/DS-663a9099-7381-4561-a66c-06d8de9e45a1/container.db";

    // read-only mode
    ManagedOptions managedOptions = new ManagedOptions();
    managedOptions.setCreateIfMissing(false);
    ManagedRocksDB storeReadonly = ManagedRocksDB.openReadOnly(managedOptions, path);
    storeReadonly.get().maxMemCompactionLevel();
    System.out.println("DB is opened");
    storeReadonly.close();
    storeReadonly.close();

    // normal mode
    try (DatanodeStore store = new DatanodeStoreSchemaThreeImpl(conf, path, false)) {
      if (!store.isClosed()) {
        store.close();
        store.flushDB();
        store.close();
      }
    } catch (Exception e) {
      System.out.println("Failed to open DB");
      e.printStackTrace();
    }
  }

  @Test
  public void testNoSpace() throws RocksDBException {
    final OzoneConfiguration conf = new OzoneConfiguration();
    String path = "/Volumes/DiskFullTest/ozone-dn/hdds/CID-b59af358-d167-4a7a-aa78-6090e624a64f/DS-663a9099-7381-4561-a66c-06d8de9e45a1/container.db";

    // Open files under /Volumes/DiskFullTest directory with names starting with "file-"
    File testDir = new File("/Volumes/DiskFullTest");
    int fileCreated = 0;
    int maxFiles = 10011;

    for (int i = 0; i < maxFiles; i++) {
      String fileName = "file-" + i;
      File file = new File(testDir, fileName);
      try {
        file.createNewFile();
        fileCreated++;
      } catch (IOException e) {
        System.out.println("Failed to create file: " + fileName);
        e.printStackTrace();
        System.out.println("Created " + fileCreated + " files");
        break;
      }
    }

    OperatingSystemMXBean os = ManagementFactory.getOperatingSystemMXBean();
    if (os instanceof UnixOperatingSystemMXBean) {
      System.out.println("Max File Descriptors: " + ((UnixOperatingSystemMXBean) os).getMaxFileDescriptorCount());
      long openCount = ((UnixOperatingSystemMXBean) os).getOpenFileDescriptorCount();
      System.out.println("Open files: " + openCount);
    }

    // read-only mode
    ManagedOptions managedOptions = new ManagedOptions();
    managedOptions.setCreateIfMissing(false);
    ManagedRocksDB storeReadonly = ManagedRocksDB.openReadOnly(managedOptions, path);
    storeReadonly.get().maxMemCompactionLevel();
    System.out.println("DB is opened");

    // normal mode
    try (DatanodeStore store = new DatanodeStoreSchemaThreeImpl(conf, path, false)) {
      if (store.isClosed()) {
        System.out.println("DB is closed");
      }
    } catch (Exception e) {
      System.out.println("Failed to open DB");
      e.printStackTrace();
      for (int i = 0; i < fileCreated; i++) {
        String fileName = "file-" + i;
        File file = new File(testDir, fileName);
        file.delete();
      }
    }
  }

  private void gcSimulator(Object lock) {
    LinkedList<Object> list = new LinkedList<>();
    System.out.println("Filling heap to create GC pressure...");

    // Step 1: Fill the heap until it's nearly full
    for (int i = 0; i < 1024 * (1024 - 50) * 8; i++) {
      list.add(new byte[1024]); // 1KB objects
    }

    MemoryMXBean memBean = ManagementFactory.getMemoryMXBean();
    MemoryUsage heapUsage = memBean.getHeapMemoryUsage();

    synchronized (lock) {
      lock.notify();
    }
    
    System.out.println("Triggering Full GC. Expect a pause...");
    getHeapUsage();

    long startTime = System.currentTimeMillis();
    // Step 2: Manually suggest a Full GC
    System.gc();

    System.out.println("List size " + list.size() + 
        ", GC finished after " + (System.currentTimeMillis() - startTime) + "ms");

    getHeapUsage();
  }

  private void getHeapUsage() {
    MemoryMXBean memBean = ManagementFactory.getMemoryMXBean();
    MemoryUsage heapUsage = memBean.getHeapMemoryUsage();

    System.out.println("Used: " + heapUsage.getUsed() / 1024 / 1024 + " MB" +
        ", Max: " + heapUsage.getMax() / 1024 / 1024 + " MB" +
        ", Committed: " + heapUsage.getCommitted() / 1024 / 1024 + " MB");
  }
  
  /**
   * Test {@link ServerUtils#getScmDbDir} with fallback to OZONE_METADATA_DIRS
   * when OZONE_SCM_DB_DIRS is undefined.
   */
  @Test
  public void testGetScmDbDirWithFallback() {
    final File testDir = PathUtils.getTestDir(TestHddsServerUtils.class);
    final File metaDir = new File(testDir, "metaDir");
    final OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(HddsConfigKeys.OZONE_METADATA_DIRS, metaDir.getPath());
    try {
      assertEquals(metaDir, ServerUtils.getScmDbDir(conf));
      assertTrue(metaDir.exists());        // should have been created.
    } finally {
      FileUtils.deleteQuietly(metaDir);
    }
  }

  @Test
  public void testNoScmDbDirConfigured() {
    assertThrows(IllegalArgumentException.class,
        () -> ServerUtils.getScmDbDir(new OzoneConfiguration()));
  }

  @Test
  public void testGetStaleNodeInterval() {
    final OzoneConfiguration conf = new OzoneConfiguration();

    // Reset OZONE_SCM_STALENODE_INTERVAL to 300s that
    // larger than max limit value.
    conf.setTimeDuration(OZONE_SCM_STALENODE_INTERVAL, 300, TimeUnit.SECONDS);
    conf.setInt(ScmConfigKeys.OZONE_SCM_HEARTBEAT_PROCESS_INTERVAL, 100);
    // the max limit value will be returned
    assertEquals(100000, HddsServerUtil.getStaleNodeInterval(conf));

    // Reset OZONE_SCM_STALENODE_INTERVAL to 10ms that
    // smaller than min limit value.
    conf.setTimeDuration(OZONE_SCM_STALENODE_INTERVAL, 10,
        TimeUnit.MILLISECONDS);
    conf.setInt(ScmConfigKeys.OZONE_SCM_HEARTBEAT_PROCESS_INTERVAL, 100);
    // the min limit value will be returned
    assertEquals(90000, HddsServerUtil.getStaleNodeInterval(conf));
  }

  @Test
  public void testGetDatanodeIdFilePath() {
    final File testDir = PathUtils.getTestDir(TestHddsServerUtils.class);
    final File metaDir = new File(testDir, "metaDir");
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(HddsConfigKeys.OZONE_METADATA_DIRS, metaDir.getPath());

    try {
      // test fallback if not set
      assertEquals(new File(metaDir,
              OZONE_SCM_DATANODE_ID_FILE_DEFAULT).toString(),
          HddsServerUtil.getDatanodeIdFilePath(conf));

      // test fallback if set empty
      conf.set(OZONE_SCM_DATANODE_ID_DIR, "");
      assertEquals(new File(metaDir,
              OZONE_SCM_DATANODE_ID_FILE_DEFAULT).toString(),
          HddsServerUtil.getDatanodeIdFilePath(conf));

      // test use specific value if set
      final File dnIdDir = new File(testDir, "datanodeIDDir");
      conf.set(OZONE_SCM_DATANODE_ID_DIR, dnIdDir.getPath());
      assertEquals(new File(dnIdDir,
              OZONE_SCM_DATANODE_ID_FILE_DEFAULT).toString(),
          HddsServerUtil.getDatanodeIdFilePath(conf));
    } finally {
      FileUtils.deleteQuietly(metaDir);
    }
  }
}
