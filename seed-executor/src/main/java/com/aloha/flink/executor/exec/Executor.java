package com.aloha.flink.executor.exec;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;

public class Executor {

    private StreamExecutionEnvironment sExeEnv;
    private StreamTableEnvironment sTblEnv;

    Executor(StreamExecutionEnvironment sExeEnv) {
        this.sExeEnv = sExeEnv;
        StreamTableEnvironment sTableEnv = StreamTableEnvironment.create(sExeEnv);
    }


}
