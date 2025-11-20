package com.taskscheduler.flink;

import com.taskscheduler.model.Task;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;

/**
 * Standalone Flink Job Main Class
 * This can be submitted directly to Flink cluster
 */
public class TaskSchedulerJobMain {
    private static final Logger log = LoggerFactory.getLogger(TaskSchedulerJobMain.class);

    public static void main(String[] args) throws Exception {
        // Configuration - you can pass these as program arguments
        String bootstrapServers = args.length > 0 ? args[0] : "localhost:9092";
        String taskEventsTopic = args.length > 1 ? args[1] : "task-events";
        String scheduledTasksTopic = args.length > 2 ? args[2] : "scheduled-tasks";
        
        log.info("Starting Flink TaskScheduler Job with:");
        log.info("Bootstrap Servers: {}", bootstrapServers);
        log.info("Input Topic: {}", taskEventsTopic);
        log.info("Output Topic: {}", scheduledTasksTopic);

        // Set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        // Enable checkpointing
        env.enableCheckpointing(10000); // 10 seconds
        
        // Set up the Kafka source
        KafkaSource<Task> source = KafkaSource.<Task>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopics(taskEventsTopic)
                .setGroupId("flink-task-scheduler")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new JsonDeserializer<>(Task.class))
                .build();

        // Create a data stream from the source
        DataStream<Task> tasks = env.fromSource(
                source,
                WatermarkStrategy.noWatermarks(),
                "Kafka Source"
        );

        // Process the tasks
        DataStream<Task> scheduledTasks = tasks
                .keyBy(task -> task.getId().toString())
                .process(new TaskSchedulerFunction())
                .name("Task Scheduler");

        // Sink the scheduled tasks back to Kafka
        KafkaSink<Task> sink = KafkaSink.<Task>builder()
                .setBootstrapServers(bootstrapServers)
                .setRecordSerializer(KafkaRecordSerializationSchema.<Task>builder()
                        .setTopic(scheduledTasksTopic)
                        .setValueSerializationSchema(new JsonSerializationSchema<Task>())
                        .build())
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();

        scheduledTasks.sinkTo(sink).name("Kafka Sink");

        // Execute the job
        env.execute("Task Scheduler Job");
    }

    /**
     * Task Scheduler Function - processes tasks and manages scheduling
     */
    public static class TaskSchedulerFunction extends KeyedProcessFunction<String, Task, Task> {
        private transient ValueState<Long> nextExecutionTimeState;

        @Override
        public void open(OpenContext openContext) throws Exception {
            ValueStateDescriptor<Long> descriptor = new ValueStateDescriptor<>(
                    "next-execution-time",
                    TypeInformation.of(Long.class)
            );
            nextExecutionTimeState = getRuntimeContext().getState(descriptor);
        }

        @Override
        public void processElement(
                Task task,
                Context ctx,
                Collector<Task> out) throws Exception {
            
            log.info("Processing task: {} with status: {}", task.getId(), task.getStatus());
            
            // Calculate next execution time based on cron expression
            // This is a simplified example - in a real implementation, you would use a cron parser
            long now = System.currentTimeMillis();
            long nextExecutionTime = now + 60000; // Default to 1 minute later
            
            // Update the next execution time
            nextExecutionTimeState.update(nextExecutionTime);
            
            // Schedule the next execution
            ctx.timerService().registerProcessingTimeTimer(nextExecutionTime);
            
            // Emit the task for immediate processing
            task.setStatus("SCHEDULED");
            task.setUpdatedAt(Instant.now());
            out.collect(task);
            
            log.info("Task scheduled: {} for execution at: {}", task.getId(), Instant.ofEpochMilli(nextExecutionTime));
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<Task> out) {
            try {
                log.info("Timer fired for key: {} at timestamp: {}", ctx.getCurrentKey(), timestamp);
                
                // Get the next execution time
                Long nextTime = nextExecutionTimeState.value();
                
                if (nextTime != null && nextTime == timestamp) {
                    // Create a task for processing
                    Task task = new Task();
                    task.setId(java.util.UUID.fromString(ctx.getCurrentKey()));
                    task.setStatus("SCHEDULED");
                    task.setUpdatedAt(Instant.ofEpochMilli(timestamp));
                    
                    // Emit the task for processing
                    out.collect(task);
                    
                    // Schedule the next execution
                    long newNextTime = timestamp + 60000; // Schedule next execution 1 minute later
                    nextExecutionTimeState.update(newNextTime);
                    ctx.timerService().registerProcessingTimeTimer(newNextTime);
                    
                    log.info("Recurring task scheduled: {} for next execution at: {}", 
                            task.getId(), Instant.ofEpochMilli(newNextTime));
                }
            } catch (Exception e) {
                log.error("Error in timer processing for key: {}", ctx.getCurrentKey(), e);
            }
        }
    }
}
