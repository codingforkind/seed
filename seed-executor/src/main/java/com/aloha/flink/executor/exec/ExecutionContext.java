package com.aloha.flink.executor.exec;

import com.aloha.flink.executor.context.session.Session;
import org.apache.flink.streaming.api.datastream.DataStream;

import java.util.HashMap;
import java.util.Map;

public class ExecutionContext {

    /**
     * key: database.table
     * value: transformed tables
     */
    private Map<String, DataStream> streamMap = new HashMap<String, DataStream>();

    private Session session;

}
