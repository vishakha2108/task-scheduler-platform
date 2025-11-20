package com.taskscheduler.controller;

import com.taskscheduler.dto.CreateTaskRequest;
import com.taskscheduler.model.Task;
import com.taskscheduler.service.TaskService;
import com.taskscheduler.service.ApiGatewayService;
import com.taskscheduler.service.WorkflowOrchestrationService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.net.URI;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

@Slf4j
@RestController
@RequestMapping("/api/tasks")
@RequiredArgsConstructor
public class TaskController {

    private final TaskService taskService;
    private final ApiGatewayService apiGatewayService;
    private final WorkflowOrchestrationService workflowOrchestrationService;

    @PostMapping
    public ResponseEntity<Task> createTask(@Valid @RequestBody CreateTaskRequest request) {
        log.info("Received task creation request: {}", request.getName());
        
        // Process through API Gateway Service (as shown in sequence diagram)
        Task task = apiGatewayService.processTaskCreationRequest(request);
        
        // Start workflow orchestration
        CompletableFuture<Task> workflowFuture = workflowOrchestrationService.orchestrateTaskWorkflow(task.getId());
        
        return ResponseEntity
                .created(URI.create("/api/tasks/" + task.getId()))
                .body(task);
    }

    @GetMapping("/health")
    public ResponseEntity<String> getHello() {

        return ResponseEntity.ok("Allah hu habibi!!! bismilaah nizamudiin sarkar maksqad qubool ho!");
    }

    @GetMapping("/{id}")
    public ResponseEntity<Task> getTask(@PathVariable UUID id) {
        Task task = taskService.getTask(id);
        return ResponseEntity.ok(task);
    }

    @PostMapping("/{id}/cancel")
    public ResponseEntity<Void> cancelTask(@PathVariable UUID id) {
        taskService.updateTaskStatus(id, "CANCELLED");
        return ResponseEntity.noContent().build();
    }

    @PostMapping("/{id}/retry")
    public ResponseEntity<Void> retryTask(@PathVariable UUID id) {
        log.info("Retrying task: {}", id);
        taskService.updateTaskStatus(id, "RETRY");
        
        // Restart workflow orchestration for retry
        workflowOrchestrationService.orchestrateTaskWorkflow(id);
        
        return ResponseEntity.accepted().build();
    }
    
    @PostMapping("/{id}/schedule")
    public ResponseEntity<Void> scheduleTask(@PathVariable UUID id) {
        log.info("Scheduling task: {}", id);
        apiGatewayService.scheduleTask(id);
        return ResponseEntity.accepted().build();
    }
    
    @PutMapping("/{id}/status")
    public ResponseEntity<Void> updateTaskStatus(@PathVariable UUID id, @RequestParam String status) {
        log.info("Updating task status: {} -> {}", id, status);
        apiGatewayService.processTaskStatusUpdate(id, status);
        return ResponseEntity.noContent().build();
    }
    
    @GetMapping("/{id}/status")
    public ResponseEntity<String> getTaskStatus(@PathVariable UUID id) {
        Task task = taskService.getTask(id);
        return ResponseEntity.ok(task.getStatus());
    }
}
