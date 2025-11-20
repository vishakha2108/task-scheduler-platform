package com.taskscheduler.repository;

import com.taskscheduler.model.Task;
import com.taskscheduler.model.TaskMetaData;
import org.springframework.data.cassandra.repository.CassandraRepository;
import org.springframework.stereotype.Repository;

import java.util.UUID;

@Repository
public interface TaskMetaDataRepository extends CassandraRepository<TaskMetaData, UUID> {
    // Basic CRUD operations provided by CassandraRepository
}
