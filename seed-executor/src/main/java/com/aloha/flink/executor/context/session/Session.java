package com.aloha.flink.executor.context.session;

import com.aloha.flink.executor.context.ExecutionContext;
import lombok.Data;

@Data
public class Session {
    private String token;
    private String topic;

    private ExecutionContext exeCtx;
}
