package com.aloha.flink.executor.context.session;

import com.aloha.flink.executor.context.ExecutionContext;
import lombok.Data;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

@Data
public class Session {
    private String token;
    private String tblSrcTopic;
    private String exeSqlTopic;

    private ExecutionContext exeCtx;
}
