package com.taskscheduler.service;

import com.taskscheduler.dto.CreateTaskRequest;
import com.taskscheduler.model.Task;
import com.taskscheduler.repository.TaskRepository;
import io.micrometer.core.annotation.Timed;
import io.micrometer.core.annotation.Counted;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@Slf4j
@Service
public class TaskService {

    private final TaskRepository taskRepository;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final String taskEventsTopic;

    public TaskService(TaskRepository taskRepository, 
                      KafkaTemplate<String, Object> kafkaTemplate,
                      @Value("${kafka.topics.task-events}") String taskEventsTopic) {
        this.taskRepository = taskRepository;
        this.kafkaTemplate = kafkaTemplate;
        this.taskEventsTopic = taskEventsTopic;
    }

    @Transactional
    @Timed(value = "taskscheduler_database_save_duration_seconds", description = "Time taken to save tasks to database")
    @Counted(value = "taskscheduler_tasks_created_total", description = "Total number of tasks created")
    public Task createTask(CreateTaskRequest request) {
        Task task = new Task();
        task.setId(UUID.randomUUID());
        task.setName(request.getName());
        task.setDescription(request.getDescription());
        task.setCronExpression(request.getCronExpression());
        task.setStatus("CREATED");
        task.setCreatedAt(Instant.now());
        task.setUpdatedAt(Instant.now());
        // Convert Map<String, Object> to Map<String, String> for Cassandra
        if (request.getParameters() != null) {
            Map<String, String> stringParams = request.getParameters().entrySet().stream()
                .collect(java.util.stream.Collectors.toMap(
                    Map.Entry::getKey,
                    entry -> entry.getValue() != null ? entry.getValue().toString() : null
                ));
            task.setParameters(stringParams);
        }
        task.setCreatedBy(request.getCreatedBy());
        task.setAssignedTo(request.getAssignedTo());
        task.setMaxRetries(request.getMaxRetries());
        task.setRetryDelayMs(request.getRetryDelayMs());

        Task savedTask = taskRepository.save(task);
        
        // Publish task creation event
        kafkaTemplate.send(taskEventsTopic, savedTask.getId().toString(), savedTask);
        
        return savedTask;
    }

    @Transactional(readOnly = true)
    @Timed(value = "taskscheduler_database_query_duration_seconds", description = "Time taken to query tasks from database")
    public Task getTask(UUID taskId) {
        return taskRepository.findById(taskId)
                .orElseThrow(() -> new RuntimeException("Task not found with id: " + taskId));
    }

    @Scheduled(fixedDelayString = "${flink.checkpoint.interval}")
    @Transactional
    public void scheduleDueTasks() {
        List<Task> dueTasks = taskRepository.findTasksDueForExecution(Instant.now());
        
        for (Task task : dueTasks) {
            try {
                task.setStatus("SCHEDULED");
                task.setUpdatedAt(Instant.now());
                taskRepository.save(task);
                
                // Publish task for execution
                kafkaTemplate.send(taskEventsTopic, task.getId().toString(), task);
                
                log.info("Scheduled task: {}", task.getId());
            } catch (Exception e) {
                log.error("Error scheduling task: {}", task.getId(), e);
            }
        }
    }

    @Transactional
    public void updateTaskStatus(UUID taskId, String status) {
        taskRepository.updateStatus(taskId, status, Instant.now());
    }
}
