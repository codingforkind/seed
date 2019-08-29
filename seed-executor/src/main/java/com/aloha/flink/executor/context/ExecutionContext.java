package com.aloha.flink.executor.context;

import lombok.Data;
import org.apache.flink.streaming.connectors.kafka.Kafka011TableSource;

import java.util.LinkedList;
import java.util.List;

@Data
public class ExecutionContext {

    private List<Kafka011TableSource> sourceList = new LinkedList();

}
