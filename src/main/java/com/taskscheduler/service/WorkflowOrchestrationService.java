package com.taskscheduler.service;

import com.taskscheduler.model.Task;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

@Slf4j
@Service
@RequiredArgsConstructor
public class WorkflowOrchestrationService {

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final TaskService taskService;
    private final FlinkEventService flinkEventService;
    
    @Value("${kafka.topics.workflow-events}")
    private String workflowEventsTopic;
    
    @Value("${kafka.topics.task-execution}")
    private String taskExecutionTopic;

    /**
     * Orchestrate the complete task workflow
     */
    public CompletableFuture<Task> orchestrateTaskWorkflow(UUID taskId) {
        log.info("Starting workflow orchestration for task: {}", taskId);
        
        return CompletableFuture.supplyAsync(() -> {
            try {
                Task task = taskService.getTask(taskId);
                
                // Step 1: Validate task
                validateTask(task);
                
                // Step 2: Prepare task for execution
                prepareTaskForExecution(task);
                
                // Step 3: Send to execution queue
                sendTaskToExecutionQueue(task);
                
                // Step 4: Monitor task execution
                monitorTaskExecution(task);
                
                return task;
                
            } catch (Exception e) {
                log.error("Error in workflow orchestration for task: {}", taskId, e);
                handleWorkflowError(taskId, e);
                throw new RuntimeException("Workflow orchestration failed", e);
            }
        });
    }

    /**
     * Listen to task requests from API Gateway
     */
    @KafkaListener(topics = "${kafka.topics.task-requests}", groupId = "workflow-orchestration")
    public void processTaskRequest(Task task) {
        log.info("Processing task request: {} with status: {}", task.getId(), task.getStatus());
        
        try {
            // Start workflow orchestration for the new task
            orchestrateTaskWorkflow(task.getId());
        } catch (Exception e) {
            log.error("Error processing task request: {}", task.getId(), e);
            handleWorkflowError(task.getId(), e);
        }
    }

    /**
     * Listen to workflow events from Kafka
     */
    @KafkaListener(topics = "${kafka.topics.workflow-events}", groupId = "workflow-orchestration")
    public void processWorkflowEvent(WorkflowEvent event) {
        log.info("Processing workflow event: {} for task: {}", event.getEventType(), event.getTaskId());
        
        try {
            switch (event.getEventType()) {
                case "TASK_SUBMITTED":
                    handleTaskSubmitted(event);
                    break;
                case "TASK_VALIDATED":
                    handleTaskValidated(event);
                    break;
                case "TASK_PREPARED":
                    handleTaskPrepared(event);
                    break;
                case "TASK_EXECUTED":
                    handleTaskExecuted(event);
                    break;
                case "TASK_COMPLETED":
                    handleTaskCompleted(event);
                    break;
                case "TASK_FAILED":
                    handleTaskFailed(event);
                    break;
                default:
                    log.warn("Unknown workflow event type: {}", event.getEventType());
            }
        } catch (Exception e) {
            log.error("Error processing workflow event: {}", event, e);
        }
    }

    /**
     * Validate task before execution
     */
    private void validateTask(Task task) {
        log.info("Validating task: {}", task.getId());
        
        // Validate required fields
        if (task.getName() == null || task.getName().trim().isEmpty()) {
            throw new IllegalArgumentException("Task name is required");
        }
        
        if (task.getCronExpression() == null || task.getCronExpression().trim().isEmpty()) {
            throw new IllegalArgumentException("Cron expression is required");
        }
        
        // Validate cron expression format
        if (!isValidCronExpression(task.getCronExpression())) {
            throw new IllegalArgumentException("Invalid cron expression: " + task.getCronExpression());
        }
        
        // Update task status
        task.setStatus("VALIDATED");
        task.setUpdatedAt(Instant.now());
        taskService.updateTaskStatus(task.getId(), "VALIDATED");
        
        // Send validation event
        sendWorkflowEvent(task.getId(), "TASK_VALIDATED", "Task validation completed successfully");
    }

    /**
     * Prepare task for execution
     */
    private void prepareTaskForExecution(Task task) {
        log.info("Preparing task for execution: {}", task.getId());
        
        // Calculate next execution time
        calculateNextExecutionTime(task);
        
        // Initialize retry counters
        task.setCurrentRetries(0);
        
        // Update task status
        task.setStatus("PREPARED");
        task.setUpdatedAt(Instant.now());
        taskService.updateTaskStatus(task.getId(), "PREPARED");
        
        // Send preparation event
        sendWorkflowEvent(task.getId(), "TASK_PREPARED", "Task preparation completed successfully");
    }

    /**
     * Send task to execution queue
     */
    private void sendTaskToExecutionQueue(Task task) {
        log.info("Sending task to execution queue: {}", task.getId());
        
        task.setStatus("QUEUED");
        task.setUpdatedAt(Instant.now());
        
        // Send to Kafka execution topic
        kafkaTemplate.send(taskExecutionTopic, task.getId().toString(), task);
        
        // Send workflow event
        sendWorkflowEvent(task.getId(), "TASK_QUEUED", "Task sent to execution queue");
    }

    /**
     * Monitor task execution
     */
    private void monitorTaskExecution(Task task) {
        log.info("Starting task execution monitoring: {}", task.getId());
        
        // This would typically involve setting up monitoring
        // For now, we'll just log and update status
        task.setStatus("MONITORING");
        task.setUpdatedAt(Instant.now());
        
        // Send monitoring event
        sendWorkflowEvent(task.getId(), "TASK_MONITORING", "Task execution monitoring started");
    }

    /**
     * Handle workflow error
     */
    private void handleWorkflowError(UUID taskId, Exception error) {
        log.error("Handling workflow error for task: {}", taskId, error);
        
        try {
            taskService.updateTaskStatus(taskId, "WORKFLOW_ERROR");
            sendWorkflowEvent(taskId, "WORKFLOW_ERROR", "Workflow error: " + error.getMessage());
        } catch (Exception e) {
            log.error("Error handling workflow error", e);
        }
    }

    /**
     * Send workflow event to Kafka
     */
    private void sendWorkflowEvent(UUID taskId, String eventType, String message) {
        WorkflowEvent event = WorkflowEvent.builder()
                .taskId(taskId)
                .eventType(eventType)
                .message(message)
                .timestamp(Instant.now())
                .build();
        
        kafkaTemplate.send(workflowEventsTopic, taskId.toString(), event);
    }

    /**
     * Validate cron expression (simplified)
     */
    private boolean isValidCronExpression(String cronExpression) {
        // Simplified validation - in real implementation use a proper cron parser
        return cronExpression != null && cronExpression.split("\\s+").length >= 5;
    }

    /**
     * Calculate next execution time based on cron expression
     */
    private void calculateNextExecutionTime(Task task) {
        // Simplified implementation - in real scenario use a proper cron parser
        // For now, just set it to 1 minute from now
        task.setNextExecutionTime(Instant.now().plusSeconds(60));
    }

    // Event handlers
    private void handleTaskSubmitted(WorkflowEvent event) {
        log.info("Handling task submitted event: {}", event.getTaskId());
        orchestrateTaskWorkflow(event.getTaskId());
    }

    private void handleTaskValidated(WorkflowEvent event) {
        log.info("Handling task validated event: {}", event.getTaskId());
        // Continue with next step in workflow
    }

    private void handleTaskPrepared(WorkflowEvent event) {
        log.info("Handling task prepared event: {}", event.getTaskId());
        // Continue with next step in workflow
    }

    private void handleTaskExecuted(WorkflowEvent event) {
        log.info("Handling task executed event: {}", event.getTaskId());
        // Process execution results
    }

    private void handleTaskCompleted(WorkflowEvent event) {
        log.info("Handling task completed event: {}", event.getTaskId());
        // Finalize task completion
    }

    private void handleTaskFailed(WorkflowEvent event) {
        log.info("Handling task failed event: {}", event.getTaskId());
        // Handle task failure and potential retry
    }

    /**
     * Workflow Event DTO
     */
    public static class WorkflowEvent {
        private UUID taskId;
        private String eventType;
        private String message;
        private Instant timestamp;
        
        public static WorkflowEventBuilder builder() {
            return new WorkflowEventBuilder();
        }
        
        public static class WorkflowEventBuilder {
            private UUID taskId;
            private String eventType;
            private String message;
            private Instant timestamp;
            
            public WorkflowEventBuilder taskId(UUID taskId) {
                this.taskId = taskId;
                return this;
            }
            
            public WorkflowEventBuilder eventType(String eventType) {
                this.eventType = eventType;
                return this;
            }
            
            public WorkflowEventBuilder message(String message) {
                this.message = message;
                return this;
            }
            
            public WorkflowEventBuilder timestamp(Instant timestamp) {
                this.timestamp = timestamp;
                return this;
            }
            
            public WorkflowEvent build() {
                WorkflowEvent event = new WorkflowEvent();
                event.taskId = this.taskId;
                event.eventType = this.eventType;
                event.message = this.message;
                event.timestamp = this.timestamp;
                return event;
            }
        }
        
        // Getters and setters
        public UUID getTaskId() { return taskId; }
        public void setTaskId(UUID taskId) { this.taskId = taskId; }
        
        public String getEventType() { return eventType; }
        public void setEventType(String eventType) { this.eventType = eventType; }
        
        public String getMessage() { return message; }
        public void setMessage(String message) { this.message = message; }
        
        public Instant getTimestamp() { return timestamp; }
        public void setTimestamp(Instant timestamp) { this.timestamp = timestamp; }
    }
}
