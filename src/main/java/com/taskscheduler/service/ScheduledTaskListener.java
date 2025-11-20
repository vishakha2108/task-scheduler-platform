package com.taskscheduler.service;

import com.taskscheduler.model.Task;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class ScheduledTaskListener {

    // Removed Kafka listener since Flink no longer sends acknowledgments back
    // @KafkaListener(topics = "${kafka.topics.scheduled-tasks}", groupId = "task-scheduler-platform")
    public void handleScheduledTask(Task task) {
        log.info("Received scheduled task from Flink: {} with status: {}", task.getId(), task.getStatus());
        log.info("Task details: name={}, description={}", task.getId(), task.getTenant());
        
        // Here you can add logic to handle the scheduled task
        // For now, just log it to show the basic flow is working
    }
}
