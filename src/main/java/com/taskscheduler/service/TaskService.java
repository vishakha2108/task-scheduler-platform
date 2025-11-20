package com.taskscheduler.service;

import com.taskscheduler.dto.CreateTaskRequest;
import com.taskscheduler.model.Task;
import com.taskscheduler.model.TaskMetaData;
import com.taskscheduler.repository.TaskMetaDataRepository;
import com.taskscheduler.repository.TaskRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.UUID;

@Slf4j
@Service
public class TaskService {

    private final TaskRepository taskRepository;
    private final TaskMetaDataRepository taskRepositoryMetaData;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final String taskRequestsTopic;

    public TaskService(TaskRepository taskRepository,TaskMetaDataRepository taskRepositoryMetaData,
                       KafkaTemplate<String, Object> kafkaTemplate,
                       @Value("${kafka.topics.task-requests}") String taskRequestsTopic) {
        this.taskRepository = taskRepository;
        this.kafkaTemplate = kafkaTemplate;
        this.taskRequestsTopic = taskRequestsTopic;
        this.taskRepositoryMetaData = taskRepositoryMetaData;
    }

    @Transactional
    public Task createTask(CreateTaskRequest request) {
        log.info("Creating task with id: {}", request.getId());

        Task task = new Task();
        task.setId(request.getId());
        task.setTenant(request.getTenant());
        task.setPayload(request.getPayload());
        task.setScheduledAt(request.getScheduledAt());
        task.setStatus(request.getStatus());

        // Save to Cassandra
        Task savedTask = taskRepository.save(task);
        log.info("Task saved to Cassandra: {}", savedTask.getId());

        // Calculate 30 days later in milliseconds
        long thirtyDaysLater = System.currentTimeMillis() + (30L * 24 * 60 * 60 * 1000);

        // Send to task-requests topic for Flink only if scheduledAt is within 30 days
        if (request.getScheduledAt() != null && request.getScheduledAt() < thirtyDaysLater) {
            kafkaTemplate.send(taskRequestsTopic, savedTask.getId(), savedTask);
            log.info("Task sent to task-requests topic: {}", savedTask.getId());
        } else {
            long bucketId = 0;
            // Set bucketId as the epoch of the day for scheduledAt
            if (request.getScheduledAt() != null) {
                long scheduledAt = request.getScheduledAt();
                bucketId = scheduledAt - (scheduledAt % (24 * 60 * 60 * 1000));
            }
            TaskMetaData taskMetaData = new TaskMetaData();
            taskMetaData.setBucketId(bucketId);
            taskMetaData.setId(request.getId());
            taskMetaData.setScheduledAt(request.getScheduledAt());
            taskRepositoryMetaData.save(taskMetaData);
            log.info("Task not sent to Kafka - scheduledAt is more than 30 days old or null: {}", savedTask.getId());
        }

        return savedTask;
    }

    @Transactional(readOnly = true)
    public Task getTask(UUID taskId) {
        return taskRepository.findById(taskId)
                .orElseThrow(() -> new RuntimeException("Task not found with id: " + taskId));
    }

    @Transactional(readOnly = true)
    public List<Task> getAllTasksSortedByTime() {
        List<Task> tasks = taskRepository.findAll();
        // Sort by scheduledAt (DESC order - newest first)
        tasks.sort((t1, t2) -> {
            if (t1.getScheduledAt() == null && t2.getScheduledAt() == null) return 0;
            if (t1.getScheduledAt() == null) return 1;
            if (t2.getScheduledAt() == null) return -1;
            return Long.compare(t2.getScheduledAt(), t1.getScheduledAt());
        });
        return tasks;
    }
}
