package com.aloha.flink.executor.context;

import lombok.Data;
import org.apache.commons.collections.map.HashedMap;
import org.apache.flink.streaming.connectors.kafka.Kafka011TableSource;

import java.util.Map;

@Data
public class ExecutionContext {

    private Map<String, Kafka011TableSource> sourceList = new HashedMap();

}
