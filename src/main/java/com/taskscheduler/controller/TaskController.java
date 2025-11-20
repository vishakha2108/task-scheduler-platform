package com.taskscheduler.controller;

import com.taskscheduler.dto.CreateTaskRequest;
import com.taskscheduler.model.Task;
import com.taskscheduler.service.TaskService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.net.URI;
import java.util.List;
import java.util.UUID;

@Slf4j
@RestController
@RequestMapping("/api/tasks")
@RequiredArgsConstructor
public class TaskController {

    private final TaskService taskService;

    @PostMapping
    public ResponseEntity<Task> createTask(@Valid @RequestBody CreateTaskRequest request) {
        log.info("Received task creation request: {}", request.getId());
        
        Task task = taskService.createTask(request);
        
        return ResponseEntity
                .created(URI.create("/api/tasks/" + task.getId()))
                .body(task);
    }

    @GetMapping("/health")
    public ResponseEntity<String> health() {
        return ResponseEntity.ok("Task Scheduler Platform - Basic Flow Working!");
    }

    @GetMapping("/{id}")
    public ResponseEntity<Task> getTask(@PathVariable UUID id) {
        Task task = taskService.getTask(id);
        return ResponseEntity.ok(task);
    }

    @GetMapping("/sorted")
    public ResponseEntity<List<Task>> getTasksSortedByTime() {
        List<Task> tasks = taskService.getAllTasksSortedByTime();
        return ResponseEntity.ok(tasks);
    }

    @GetMapping("/debug/timestamp-id")
    public ResponseEntity<String> generateTimestampId() {
        // Show what the timestamp ID looks like
        java.time.LocalDateTime now = java.time.LocalDateTime.now();
        long millis = System.currentTimeMillis() % 1000;
        
        String timestampId = String.format("%04d%02d%02d_%02d%02d%02d_%03d",
                now.getYear(),
                now.getMonthValue(),
                now.getDayOfMonth(),
                now.getHour(),
                now.getMinute(),
                now.getSecond(),
                millis);
        
        return ResponseEntity.ok(String.format("Timestamp String: %s%nThis will be stored directly in Cassandra!", timestampId));
    }
}
