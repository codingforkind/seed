package com.aloha.flink.executor.exec;


import com.alibaba.fastjson.JSON;
import com.aloha.flink.common.protocol.Tbl;
import com.aloha.flink.executor.factory.PropertyFactory;
import com.aloha.flink.executor.session.Session;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

/**
 * 注册表信息，生成Table Source，并注册到该Session的环境中。
 */
@Slf4j
public class TableRegister {

    public void register(Session session) throws Exception {

        FlinkKafkaConsumer011 fKfkConsumer011 = new FlinkKafkaConsumer011(session.getTopic(),
                new SimpleStringSchema(), PropertyFactory.genKfkProperties());

        final StreamExecutionEnvironment sEnv = StreamExecutionEnvironment.createLocalEnvironment();
        sEnv.addSource(fKfkConsumer011).map(new MapFunction<String, String>() {
            public String map(String value) throws Exception {
                System.out.println(value);
                Tbl tbl = JSON.parseObject(value, Tbl.class);
                log.info("Table info: {} {}", tbl.getSchema(), tbl.getTblName());
                return null;
            }
        });

        sEnv.execute();
    }

}
