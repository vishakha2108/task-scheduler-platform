package com.taskscheduler.model;

import lombok.Data;
import org.springframework.data.cassandra.core.mapping.Column;
import org.springframework.data.cassandra.core.mapping.PrimaryKey;
import org.springframework.data.cassandra.core.mapping.Table;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

@Data
@Table("tasks")
public class Task {
    @PrimaryKey
    private UUID id;
    
    private String name;
    private String description;
    private String status;  // CREATED, SCHEDULED, RUNNING, COMPLETED, FAILED
    
    @Column("cron_expression")
    private String cronExpression;
    
    @Column("next_execution_time")
    private Instant nextExecutionTime;
    
    @Column("created_at")
    private Instant createdAt;
    
    @Column("updated_at")
    private Instant updatedAt;
    
    private Map<String, String> parameters;
    
    @Column("created_by")
    private String createdBy;
    
    @Column("assigned_to")
    private String assignedTo;
    
    @Column("retry_count")
    private int retryCount;
    
    @Column("current_retries")
    private int currentRetries;
    
    @Column("max_retries")
    private int maxRetries;
    
    @Column("retry_delay_ms")
    private long retryDelayMs;
    
    @Column("execution_result")
    private String executionResult;
    
    @Column("error_message")
    private String errorMessage;
}
