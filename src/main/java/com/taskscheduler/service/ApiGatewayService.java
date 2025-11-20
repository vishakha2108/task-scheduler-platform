package com.taskscheduler.service;

import com.taskscheduler.dto.CreateTaskRequest;
import com.taskscheduler.model.Task;
import io.micrometer.core.annotation.Timed;
import io.micrometer.core.annotation.Counted;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
public class ApiGatewayService {

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final TaskService taskService;
    
    @Value("${kafka.topics.task-requests}")
    private String taskRequestsTopic;
    
    @Value("${kafka.topics.task-responses}")
    private String taskResponsesTopic;

    /**
     * Process incoming task creation request from client
     */
    @Timed(value = "taskscheduler_kafka_publish_duration_seconds", description = "Time taken to publish messages to Kafka")
    @Counted(value = "taskscheduler_kafka_messages_published_total", description = "Total number of messages published to Kafka")
    public Task processTaskCreationRequest(CreateTaskRequest request) {
        log.info("Processing task creation request: {}", request.getName());
        
        try {
            // Create task through TaskService
            Task createdTask = taskService.createTask(request);
            
            // Send task creation event to Kafka for further processing
            kafkaTemplate.send(taskRequestsTopic, createdTask.getId().toString(), createdTask);
            
            log.info("Task created and sent to Kafka: {}", createdTask.getId());
            return createdTask;
            
        } catch (Exception e) {
            log.error("Error processing task creation request", e);
            throw new RuntimeException("Failed to process task creation request", e);
        }
    }

    /**
     * Process task status updates
     */
    public void processTaskStatusUpdate(UUID taskId, String status) {
        log.info("Processing task status update: {} -> {}", taskId, status);
        
        try {
            taskService.updateTaskStatus(taskId, status);
            
            // Send status update event to Kafka
            Task updatedTask = taskService.getTask(taskId);
            kafkaTemplate.send(taskResponsesTopic, taskId.toString(), updatedTask);
            
        } catch (Exception e) {
            log.error("Error processing task status update for task: {}", taskId, e);
            throw new RuntimeException("Failed to process task status update", e);
        }
    }

    /**
     * Handle task scheduling requests
     */
    public void scheduleTask(UUID taskId) {
        log.info("Scheduling task: {}", taskId);
        
        try {
            Task task = taskService.getTask(taskId);
            task.setStatus("SCHEDULED");
            
            // Send to Flink for processing
            kafkaTemplate.send(taskRequestsTopic, taskId.toString(), task);
            
        } catch (Exception e) {
            log.error("Error scheduling task: {}", taskId, e);
            throw new RuntimeException("Failed to schedule task", e);
        }
    }
}
