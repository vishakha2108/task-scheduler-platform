package com.taskscheduler.service;

import com.taskscheduler.config.RocksDBMetricsConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.time.Duration;

/**
 * Service to handle RocksDB events and update corresponding metrics
 * This service acts as a bridge between RocksDB operations and the metrics system
 */
@Service
public class RocksDBEventService {

    @Autowired
    private RocksDBMetricsConfig.RocksDBMetrics rocksDBMetrics;

    /**
     * Called when a GET operation is performed
     */
    public void onGetOperation(String key, long durationMs, boolean found) {
        rocksDBMetrics.incrementReads();
        rocksDBMetrics.recordGetDuration(Duration.ofMillis(durationMs));
        
        // Log for debugging
        System.out.println("RocksDB GET: key=" + key + ", duration=" + durationMs + "ms, found=" + found);
    }

    /**
     * Called when a PUT operation is performed
     */
    public void onPutOperation(String key, byte[] value, long durationMs) {
        rocksDBMetrics.incrementWrites();
        rocksDBMetrics.addBytesWritten(value != null ? value.length : 0);
        rocksDBMetrics.recordPutDuration(Duration.ofMillis(durationMs));
        
        // Log for debugging
        System.out.println("RocksDB PUT: key=" + key + ", bytes=" + (value != null ? value.length : 0) + ", duration=" + durationMs + "ms");
    }

    /**
     * Called when a DELETE operation is performed
     */
    public void onDeleteOperation(String key, long durationMs) {
        rocksDBMetrics.incrementDeletes();
        rocksDBMetrics.recordDeleteDuration(Duration.ofMillis(durationMs));
        
        // Log for debugging
        System.out.println("RocksDB DELETE: key=" + key + ", duration=" + durationMs + "ms");
    }

    /**
     * Called when a compaction starts
     */
    public void onCompactionStart(String level, long inputBytes) {
        System.out.println("RocksDB compaction started: level=" + level + ", inputBytes=" + inputBytes);
    }

    /**
     * Called when a compaction completes
     */
    public void onCompactionComplete(String level, long inputBytes, long outputBytes, long durationMs) {
        rocksDBMetrics.addCompactionRead(inputBytes);
        rocksDBMetrics.addCompactionWrite(outputBytes);
        rocksDBMetrics.recordCompactionDuration(Duration.ofMillis(durationMs));
        
        // Log for debugging
        System.out.println("RocksDB compaction completed: level=" + level + 
                          ", input=" + inputBytes + "B, output=" + outputBytes + "B, duration=" + durationMs + "ms");
    }

    /**
     * Called when a memtable flush occurs
     */
    public void onFlushOperation(long bytesWritten, long durationMs) {
        rocksDBMetrics.incrementFlushes();
        rocksDBMetrics.recordFlushDuration(Duration.ofMillis(durationMs));
        
        // Log for debugging
        System.out.println("RocksDB flush: bytes=" + bytesWritten + ", duration=" + durationMs + "ms");
    }

    /**
     * Called when a write stall occurs
     */
    public void onWriteStall(String reason, long durationMs) {
        rocksDBMetrics.incrementStalls();
        
        // Log for debugging
        System.out.println("RocksDB write stall: reason=" + reason + ", duration=" + durationMs + "ms");
    }

    /**
     * Update RocksDB statistics - typically called periodically
     */
    public void updateRocksDBStats(long numKeys, long sstFilesSize, long liveDataSize,
                                  long memTableSize, long blockCacheUsage, long blockCacheSize,
                                  long immutableMemTables, long runningCompactions, long runningFlushes) {
        
        rocksDBMetrics.updateStateMetrics(
            numKeys, sstFilesSize, liveDataSize, memTableSize,
            blockCacheUsage, blockCacheSize, immutableMemTables,
            runningCompactions, runningFlushes
        );
    }

    /**
     * Simulate RocksDB activity for testing purposes
     */
    public void simulateRocksDBActivity() {
        // Simulate various operations
        simulateReadOperations();
        simulateWriteOperations();
        simulateBackgroundOperations();
        updateSimulatedStats();
    }

    private void simulateReadOperations() {
        // Simulate GET operations
        int numReads = (int) (Math.random() * 5) + 1; // 1-5 reads
        for (int i = 0; i < numReads; i++) {
            String key = "task_state_" + (int) (Math.random() * 1000);
            long duration = (long) (Math.random() * 10) + 1; // 1-10ms
            boolean found = Math.random() > 0.1; // 90% hit rate
            onGetOperation(key, duration, found);
        }
    }

    private void simulateWriteOperations() {
        // Simulate PUT operations
        int numWrites = (int) (Math.random() * 3) + 1; // 1-3 writes
        for (int i = 0; i < numWrites; i++) {
            String key = "task_state_" + (int) (Math.random() * 1000);
            byte[] value = new byte[(int) (Math.random() * 1024) + 100]; // 100-1124 bytes
            long duration = (long) (Math.random() * 15) + 2; // 2-17ms
            onPutOperation(key, value, duration);
        }

        // Occasional DELETE operations
        if (Math.random() < 0.2) { // 20% chance
            String key = "task_state_" + (int) (Math.random() * 1000);
            long duration = (long) (Math.random() * 8) + 1; // 1-9ms
            onDeleteOperation(key, duration);
        }
    }

    private void simulateBackgroundOperations() {
        // Simulate compaction (less frequent)
        if (Math.random() < 0.1) { // 10% chance
            String level = "L" + (int) (Math.random() * 6); // L0-L5
            long inputBytes = (long) (Math.random() * 10_000_000) + 1_000_000; // 1-11MB
            long outputBytes = (long) (inputBytes * (0.7 + Math.random() * 0.3)); // 70-100% of input
            long duration = (long) (Math.random() * 5000) + 500; // 500-5500ms
            
            onCompactionComplete(level, inputBytes, outputBytes, duration);
        }

        // Simulate flush (less frequent)
        if (Math.random() < 0.15) { // 15% chance
            long bytesWritten = (long) (Math.random() * 5_000_000) + 1_000_000; // 1-6MB
            long duration = (long) (Math.random() * 1000) + 100; // 100-1100ms
            onFlushOperation(bytesWritten, duration);
        }

        // Simulate write stall (rare)
        if (Math.random() < 0.02) { // 2% chance
            String[] reasons = {"L0 files limit", "Pending compaction bytes", "Memtable limit"};
            String reason = reasons[(int) (Math.random() * reasons.length)];
            long duration = (long) (Math.random() * 500) + 50; // 50-550ms
            onWriteStall(reason, duration);
        }
    }

    private void updateSimulatedStats() {
        // Generate realistic RocksDB statistics
        long baseNumKeys = 100_000;
        long variation = (long) (Math.random() * 20_000) - 10_000; // ±10k variation
        long numKeys = baseNumKeys + variation;

        long sstFilesSize = numKeys * 150; // ~150 bytes per key average
        long liveDataSize = (long) (sstFilesSize * (0.8 + Math.random() * 0.2)); // 80-100% of SST size
        long memTableSize = (long) (Math.random() * 64_000_000) + 16_000_000; // 16-80MB
        
        long blockCacheSize = 256_000_000; // 256MB cache size
        long blockCacheUsage = (long) (blockCacheSize * (0.3 + Math.random() * 0.5)); // 30-80% usage
        
        long immutableMemTables = (long) (Math.random() * 3); // 0-2
        long runningCompactions = (long) (Math.random() * 2); // 0-1
        long runningFlushes = (long) (Math.random() * 2); // 0-1

        updateRocksDBStats(numKeys, sstFilesSize, liveDataSize, memTableSize,
                          blockCacheUsage, blockCacheSize, immutableMemTables,
                          runningCompactions, runningFlushes);
    }

    /**
     * Simulate read-heavy workload
     */
    public void simulateReadHeavyWorkload() {
        for (int i = 0; i < 10; i++) {
            String key = "hot_key_" + (int) (Math.random() * 100);
            long duration = (long) (Math.random() * 5) + 1; // 1-6ms (fast reads)
            onGetOperation(key, duration, true);
        }
    }

    /**
     * Simulate write-heavy workload with potential stalls
     */
    public void simulateWriteHeavyWorkload() {
        for (int i = 0; i < 5; i++) {
            String key = "batch_key_" + System.currentTimeMillis() + "_" + i;
            byte[] value = new byte[2048]; // 2KB values
            long duration = (long) (Math.random() * 25) + 5; // 5-30ms (slower writes)
            onPutOperation(key, value, duration);
        }

        // Higher chance of write stall during heavy writes
        if (Math.random() < 0.3) {
            onWriteStall("Heavy write load", (long) (Math.random() * 200) + 50);
        }
    }
}