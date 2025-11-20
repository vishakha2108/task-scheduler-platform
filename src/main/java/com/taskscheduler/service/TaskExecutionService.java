package com.taskscheduler.service;

import com.taskscheduler.model.Task;
import io.micrometer.core.annotation.Timed;
import io.micrometer.core.annotation.Counted;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Slf4j
@Service
@RequiredArgsConstructor
public class TaskExecutionService {

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final TaskService taskService;
    private final ExecutorService executorService = Executors.newFixedThreadPool(4);
    
    @Value("${kafka.topics.task-events}")
    private String taskEventsTopic;
    
    @Value("${kafka.topics.task-notifications}")
    private String taskNotificationsTopic;

    /**
     * Listen to task execution requests from Kafka
     */
    @KafkaListener(topics = "${kafka.topics.task-execution}", groupId = "task-execution-service")
    @Timed(value = "taskscheduler_task_execution_duration_seconds", description = "Time taken to execute tasks")
    @Counted(value = "taskscheduler_kafka_messages_consumed_total", description = "Total number of messages consumed from Kafka")
    public void executeTask(Task task) {
        log.info("Received task for execution: {}", task.getId());
        
        CompletableFuture.runAsync(() -> {
            try {
                performTaskExecution(task);
            } catch (Exception e) {
                log.error("Error executing task: {}", task.getId(), e);
                handleTaskExecutionError(task, e);
            }
        }, executorService);
    }

    /**
     * Perform the actual task execution
     */
    private void performTaskExecution(Task task) {
        log.info("Starting execution of task: {}", task.getId());
        
        try {
            // Update task status to RUNNING
            updateTaskStatus(task, "RUNNING");
            
            // Simulate task execution based on task parameters
            executeTaskLogic(task);
            
            // Mark task as completed
            completeTask(task);
            
        } catch (Exception e) {
            log.error("Task execution failed: {}", task.getId(), e);
            failTask(task, e);
        }
    }

    /**
     * Execute the actual task logic
     */
    private void executeTaskLogic(Task task) throws Exception {
        log.info("Executing task logic for: {}", task.getId());
        
        // Simulate different types of task execution based on parameters
        String taskType = (String) task.getParameters().getOrDefault("type", "default");
        
        switch (taskType.toLowerCase()) {
            case "data_processing":
                executeDataProcessingTask(task);
                break;
            case "notification":
                executeNotificationTask(task);
                break;
            case "cleanup":
                executeCleanupTask(task);
                break;
            case "report":
                executeReportTask(task);
                break;
            default:
                executeDefaultTask(task);
        }
    }

    /**
     * Execute data processing task
     */
    private void executeDataProcessingTask(Task task) throws Exception {
        log.info("Executing data processing task: {}", task.getId());
        
        // Simulate data processing
        Thread.sleep(2000); // Simulate processing time
        
        task.setExecutionResult("Data processing completed successfully");
        log.info("Data processing task completed: {}", task.getId());
    }

    /**
     * Execute notification task
     */
    private void executeNotificationTask(Task task) throws Exception {
        log.info("Executing notification task: {}", task.getId());
        
        // Simulate sending notification
        String recipient = (String) task.getParameters().getOrDefault("recipient", "default@example.com");
        String message = (String) task.getParameters().getOrDefault("message", "Default notification");
        
        // Simulate notification sending
        Thread.sleep(1000);
        
        task.setExecutionResult("Notification sent to: " + recipient);
        log.info("Notification task completed: {}", task.getId());
    }

    /**
     * Execute cleanup task
     */
    private void executeCleanupTask(Task task) throws Exception {
        log.info("Executing cleanup task: {}", task.getId());
        
        // Simulate cleanup operations
        Thread.sleep(1500);
        
        task.setExecutionResult("Cleanup operations completed");
        log.info("Cleanup task completed: {}", task.getId());
    }

    /**
     * Execute report task
     */
    private void executeReportTask(Task task) throws Exception {
        log.info("Executing report task: {}", task.getId());
        
        // Simulate report generation
        Thread.sleep(3000);
        
        task.setExecutionResult("Report generated successfully");
        log.info("Report task completed: {}", task.getId());
    }

    /**
     * Execute default task
     */
    private void executeDefaultTask(Task task) throws Exception {
        log.info("Executing default task: {}", task.getId());
        
        // Simulate default task execution
        Thread.sleep(1000);
        
        task.setExecutionResult("Default task execution completed");
        log.info("Default task completed: {}", task.getId());
    }

    /**
     * Update task status
     */
    private void updateTaskStatus(Task task, String status) {
        task.setStatus(status);
        task.setUpdatedAt(Instant.now());
        
        // Update in database
        taskService.updateTaskStatus(task.getId(), status);
        
        // Send status update event
        kafkaTemplate.send(taskEventsTopic, task.getId().toString(), task);
        
        log.info("Task status updated: {} -> {}", task.getId(), status);
    }

    /**
     * Complete task execution
     */
    private void completeTask(Task task) {
        log.info("Completing task: {}", task.getId());
        
        updateTaskStatus(task, "COMPLETED");
        
        // Send completion notification
        sendTaskNotification(task, "Task completed successfully: " + task.getExecutionResult());
    }

    /**
     * Handle task execution failure
     */
    private void failTask(Task task, Exception error) {
        log.error("Failing task: {}", task.getId(), error);
        
        task.setErrorMessage(error.getMessage());
        updateTaskStatus(task, "FAILED");
        
        // Send failure notification
        sendTaskNotification(task, "Task failed: " + error.getMessage());
    }

    /**
     * Handle task execution error
     */
    private void handleTaskExecutionError(Task task, Exception error) {
        log.error("Handling task execution error: {}", task.getId(), error);
        
        // Check if task should be retried
        if (shouldRetryTask(task)) {
            retryTask(task);
        } else {
            failTask(task, error);
        }
    }

    /**
     * Check if task should be retried
     */
    private boolean shouldRetryTask(Task task) {
        return task.getCurrentRetries() < task.getMaxRetries();
    }

    /**
     * Retry task execution
     */
    private void retryTask(Task task) {
        log.info("Retrying task: {} (attempt {}/{})", 
                task.getId(), task.getCurrentRetries() + 1, task.getMaxRetries());
        
        task.setCurrentRetries(task.getCurrentRetries() + 1);
        task.setStatus("RETRYING");
        task.setUpdatedAt(Instant.now());
        
        // Schedule retry after delay
        CompletableFuture.runAsync(() -> {
            try {
                Thread.sleep(task.getRetryDelayMs());
                performTaskExecution(task);
            } catch (Exception e) {
                log.error("Retry failed for task: {}", task.getId(), e);
                handleTaskExecutionError(task, e);
            }
        }, executorService);
    }

    /**
     * Send task notification
     */
    private void sendTaskNotification(Task task, String message) {
        TaskNotification notification = new TaskNotification();
        notification.setTaskId(task.getId());
        notification.setMessage(message);
        notification.setTimestamp(Instant.now());
        notification.setStatus(task.getStatus());
        
        kafkaTemplate.send(taskNotificationsTopic, task.getId().toString(), notification);
        log.info("Task notification sent: {}", message);
    }

    /**
     * Task notification DTO
     */
    public static class TaskNotification {
        private UUID taskId;
        private String message;
        private Instant timestamp;
        private String status;
        
        // Getters and setters
        public UUID getTaskId() { return taskId; }
        public void setTaskId(UUID taskId) { this.taskId = taskId; }
        
        public String getMessage() { return message; }
        public void setMessage(String message) { this.message = message; }
        
        public Instant getTimestamp() { return timestamp; }
        public void setTimestamp(Instant timestamp) { this.timestamp = timestamp; }
        
        public String getStatus() { return status; }
        public void setStatus(String status) { this.status = status; }
    }
}
