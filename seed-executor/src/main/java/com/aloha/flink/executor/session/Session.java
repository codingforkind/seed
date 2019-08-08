package com.aloha.flink.executor.session;

import lombok.Data;

@Data
public class Session {
    private String token;
    private String topic;
}
