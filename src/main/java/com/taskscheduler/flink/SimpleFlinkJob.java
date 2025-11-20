package com.taskscheduler.flink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Simple Flink Job to test deployment
 */
public class SimpleFlinkJob {
    
    public static void main(String[] args) throws Exception {
        // Set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        // Create a simple data stream
        DataStream<String> text = env.fromElements(
            "Task Scheduler Job 1",
            "Task Scheduler Job 2", 
            "Task Scheduler Job 3"
        );
        
        // Transform the data
        DataStream<String> processed = text.map(new MapFunction<String, String>() {
            @Override
            public String map(String value) throws Exception {
                return "Processed: " + value + " at " + System.currentTimeMillis();
            }
        });
        
        // Print the results
        processed.print();
        
        // Execute the job
        env.execute("Simple Task Scheduler Test Job");
    }
}
