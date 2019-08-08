package com.aloha.flink.executor.exec;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Properties;

/**
 * 把TableSource转化成DataStream备用。
 * 消息类型：字典消息，SQL消息，结果消息
 * 1. 根据字典消息生成Table Source
 * 2. SQL消息发起执行信号
 * 3. 结果消息承载计算结果信息
 */
public class DataStreamTransformer {
    private Properties kfkProps;

    StreamExecutionEnvironment eEnv = StreamExecutionEnvironment.getExecutionEnvironment();

    public DataStreamTransformer(Properties kfkProps) {
        this.kfkProps = kfkProps;
    }

    public void execute() {

        try {
            eEnv.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
