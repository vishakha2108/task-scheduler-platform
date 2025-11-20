package com.taskscheduler.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Component to periodically update RocksDB metrics for demonstration
 * This simulates RocksDB operations and updates metrics that will appear in actuator/prometheus
 */
@Component
public class RocksDBMetricsSimulator {

    @Autowired
    private RocksDBEventService rocksDBEventService;

    /**
     * Simulate normal RocksDB activity every 15 seconds
     * This will generate metrics that appear on /actuator/prometheus endpoint
     */
    @Scheduled(fixedRate = 15000) // Every 15 seconds
    public void simulateNormalActivity() {
        rocksDBEventService.simulateRocksDBActivity();
    }

    /**
     * Simulate read-heavy workload every 45 seconds
     */
    @Scheduled(fixedRate = 45000) // Every 45 seconds
    public void simulateReadHeavyWorkload() {
        rocksDBEventService.simulateReadHeavyWorkload();
    }

    /**
     * Simulate write-heavy workload every 60 seconds
     */
    @Scheduled(fixedRate = 60000) // Every 60 seconds
    public void simulateWriteHeavyWorkload() {
        rocksDBEventService.simulateWriteHeavyWorkload();
    }
}