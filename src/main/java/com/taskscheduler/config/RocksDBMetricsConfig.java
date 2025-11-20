package com.taskscheduler.config;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Configuration for RocksDB metrics that will be exposed
 * via the /actuator/prometheus endpoint
 */
@Configuration
public class RocksDBMetricsConfig {

    @Component
    public static class RocksDBMetrics {

        // RocksDB State
        private final AtomicLong estimateNumKeys = new AtomicLong(0);
        private final AtomicLong totalSstFilesSize = new AtomicLong(0);
        private final AtomicLong liveDataSize = new AtomicLong(0);
        private final AtomicLong memTableSize = new AtomicLong(0);
        private final AtomicLong blockCacheUsage = new AtomicLong(0);
        private final AtomicLong blockCacheSize = new AtomicLong(0);
        private final AtomicLong numImmutableMemTable = new AtomicLong(0);
        private final AtomicLong numRunningCompactions = new AtomicLong(0);
        private final AtomicLong numRunningFlushes = new AtomicLong(0);

        // Counters
        private final Counter rocksdbReads;
        private final Counter rocksdbWrites;
        private final Counter rocksdbDeletes;
        private final Counter rocksdbBytesRead;
        private final Counter rocksdbBytesWritten;
        private final Counter rocksdbCompactionRead;
        private final Counter rocksdbCompactionWrite;
        private final Counter rocksdbFlushes;
        private final Counter rocksdbStalls;

        // Timers
        private final Timer rocksdbGetDuration;
        private final Timer rocksdbPutDuration;
        private final Timer rocksdbDeleteDuration;
        private final Timer rocksdbCompactionTime;
        private final Timer rocksdbFlushTime;

        public RocksDBMetrics(MeterRegistry meterRegistry) {
            // Initialize Counters
            this.rocksdbReads = Counter.builder("rocksdb_reads_total")
                    .description("Total number of read operations")
                    .tag("application", "task-scheduler")
                    .register(meterRegistry);

            this.rocksdbWrites = Counter.builder("rocksdb_writes_total")
                    .description("Total number of write operations")
                    .tag("application", "task-scheduler")
                    .register(meterRegistry);

            this.rocksdbDeletes = Counter.builder("rocksdb_deletes_total")
                    .description("Total number of delete operations")
                    .tag("application", "task-scheduler")
                    .register(meterRegistry);

            this.rocksdbBytesRead = Counter.builder("rocksdb_bytes_read_total")
                    .description("Total bytes read from RocksDB")
                    .tag("application", "task-scheduler")
                    .register(meterRegistry);

            this.rocksdbBytesWritten = Counter.builder("rocksdb_bytes_written_total")
                    .description("Total bytes written to RocksDB")
                    .tag("application", "task-scheduler")
                    .register(meterRegistry);

            this.rocksdbCompactionRead = Counter.builder("rocksdb_compaction_read_total")
                    .description("Total bytes read during compaction")
                    .tag("application", "task-scheduler")
                    .register(meterRegistry);

            this.rocksdbCompactionWrite = Counter.builder("rocksdb_compaction_write_total")
                    .description("Total bytes written during compaction")
                    .tag("application", "task-scheduler")
                    .register(meterRegistry);

            this.rocksdbFlushes = Counter.builder("rocksdb_flushes_total")
                    .description("Total number of memtable flushes")
                    .tag("application", "task-scheduler")
                    .register(meterRegistry);

            this.rocksdbStalls = Counter.builder("rocksdb_stalls_total")
                    .description("Total number of write stalls")
                    .tag("application", "task-scheduler")
                    .register(meterRegistry);

            // Initialize Timers
            this.rocksdbGetDuration = Timer.builder("rocksdb_get_duration_seconds")
                    .description("Duration of GET operations")
                    .tag("application", "task-scheduler")
                    .register(meterRegistry);

            this.rocksdbPutDuration = Timer.builder("rocksdb_put_duration_seconds")
                    .description("Duration of PUT operations")
                    .tag("application", "task-scheduler")
                    .register(meterRegistry);

            this.rocksdbDeleteDuration = Timer.builder("rocksdb_delete_duration_seconds")
                    .description("Duration of DELETE operations")
                    .tag("application", "task-scheduler")
                    .register(meterRegistry);

            this.rocksdbCompactionTime = Timer.builder("rocksdb_compaction_duration_seconds")
                    .description("Duration of compaction operations")
                    .tag("application", "task-scheduler")
                    .register(meterRegistry);

            this.rocksdbFlushTime = Timer.builder("rocksdb_flush_duration_seconds")
                    .description("Duration of memtable flush operations")
                    .tag("application", "task-scheduler")
                    .register(meterRegistry);

            // Initialize Gauges
            initializeGauges(meterRegistry);
        }

        private void initializeGauges(MeterRegistry meterRegistry) {
            Gauge.builder("rocksdb_estimate_num_keys", () -> estimateNumKeys.get())
                    .description("Estimated number of keys in RocksDB")
                    .tag("application", "task-scheduler")
                    .register(meterRegistry);

            Gauge.builder("rocksdb_total_sst_files_size_bytes", () -> totalSstFilesSize.get())
                    .description("Total size of SST files in bytes")
                    .tag("application", "task-scheduler")
                    .register(meterRegistry);

            Gauge.builder("rocksdb_live_data_size_bytes", () -> liveDataSize.get())
                    .description("Live data size in bytes")
                    .tag("application", "task-scheduler")
                    .register(meterRegistry);

            Gauge.builder("rocksdb_memtable_size_bytes", () -> memTableSize.get())
                    .description("Current memtable size in bytes")
                    .tag("application", "task-scheduler")
                    .register(meterRegistry);

            Gauge.builder("rocksdb_block_cache_usage_bytes", () -> blockCacheUsage.get())
                    .description("Block cache usage in bytes")
                    .tag("application", "task-scheduler")
                    .register(meterRegistry);

            Gauge.builder("rocksdb_block_cache_size_bytes", () -> blockCacheSize.get())
                    .description("Block cache size in bytes")
                    .tag("application", "task-scheduler")
                    .register(meterRegistry);

            Gauge.builder("rocksdb_num_immutable_memtables", () -> numImmutableMemTable.get())
                    .description("Number of immutable memtables")
                    .tag("application", "task-scheduler")
                    .register(meterRegistry);

            Gauge.builder("rocksdb_num_running_compactions", () -> numRunningCompactions.get())
                    .description("Number of currently running compactions")
                    .tag("application", "task-scheduler")
                    .register(meterRegistry);

            Gauge.builder("rocksdb_num_running_flushes", () -> numRunningFlushes.get())
                    .description("Number of currently running flushes")
                    .tag("application", "task-scheduler")
                    .register(meterRegistry);
        }

        // Public methods to update metrics

        public void incrementReads() {
            rocksdbReads.increment();
        }

        public void incrementWrites() {
            rocksdbWrites.increment();
        }

        public void incrementDeletes() {
            rocksdbDeletes.increment();
        }

        public void addBytesRead(long bytes) {
            rocksdbBytesRead.increment(bytes);
        }

        public void addBytesWritten(long bytes) {
            rocksdbBytesWritten.increment(bytes);
        }

        public void addCompactionRead(long bytes) {
            rocksdbCompactionRead.increment(bytes);
        }

        public void addCompactionWrite(long bytes) {
            rocksdbCompactionWrite.increment(bytes);
        }

        public void incrementFlushes() {
            rocksdbFlushes.increment();
        }

        public void incrementStalls() {
            rocksdbStalls.increment();
        }

        public void recordGetDuration(Duration duration) {
            rocksdbGetDuration.record(duration);
        }

        public void recordPutDuration(Duration duration) {
            rocksdbPutDuration.record(duration);
        }

        public void recordDeleteDuration(Duration duration) {
            rocksdbDeleteDuration.record(duration);
        }

        public void recordCompactionDuration(Duration duration) {
            rocksdbCompactionTime.record(duration);
        }

        public void recordFlushDuration(Duration duration) {
            rocksdbFlushTime.record(duration);
        }

        // Update state metrics
        public void updateEstimateNumKeys(long keys) {
            estimateNumKeys.set(keys);
        }

        public void updateTotalSstFilesSize(long size) {
            totalSstFilesSize.set(size);
        }

        public void updateLiveDataSize(long size) {
            liveDataSize.set(size);
        }

        public void updateMemTableSize(long size) {
            memTableSize.set(size);
        }

        public void updateBlockCacheUsage(long usage) {
            blockCacheUsage.set(usage);
        }

        public void updateBlockCacheSize(long size) {
            blockCacheSize.set(size);
        }

        public void updateNumImmutableMemTable(long count) {
            numImmutableMemTable.set(count);
        }

        public void updateNumRunningCompactions(long count) {
            numRunningCompactions.set(count);
        }

        public void updateNumRunningFlushes(long count) {
            numRunningFlushes.set(count);
        }

        // Utility method to update multiple state metrics at once
        public void updateStateMetrics(long numKeys, long sstSize, long liveSize, 
                                     long memSize, long cacheUsage, long cacheSize,
                                     long immutableMem, long compactions, long flushes) {
            updateEstimateNumKeys(numKeys);
            updateTotalSstFilesSize(sstSize);
            updateLiveDataSize(liveSize);
            updateMemTableSize(memSize);
            updateBlockCacheUsage(cacheUsage);
            updateBlockCacheSize(cacheSize);
            updateNumImmutableMemTable(immutableMem);
            updateNumRunningCompactions(compactions);
            updateNumRunningFlushes(flushes);
        }
    }
}