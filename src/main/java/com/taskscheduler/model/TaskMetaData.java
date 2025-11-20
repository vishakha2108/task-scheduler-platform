package com.taskscheduler.model;

import lombok.Data;
import org.springframework.data.cassandra.core.cql.PrimaryKeyType;
import org.springframework.data.cassandra.core.mapping.Column;
import org.springframework.data.cassandra.core.mapping.PrimaryKey;
import org.springframework.data.cassandra.core.mapping.PrimaryKeyColumn;
import org.springframework.data.cassandra.core.mapping.Table;

@Data
@Table("tasksmetadata")
public class TaskMetaData {
 @PrimaryKeyColumn(name = "bucket_id", type = PrimaryKeyType.PARTITIONED)
    private Long bucketId;

    @PrimaryKeyColumn(name = "id", type = PrimaryKeyType.CLUSTERED, ordinal = 0)
    private String id;

    @Column("tenant")
    private String tenant;

    @Column("scheduled_at")
    private Long scheduledAt;

}
