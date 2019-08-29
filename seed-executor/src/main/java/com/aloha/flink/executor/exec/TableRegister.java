package com.aloha.flink.executor.exec;


import com.alibaba.fastjson.JSON;
import com.aloha.flink.common.protocol.Tbl;
import com.aloha.flink.executor.context.session.Session;
import com.aloha.flink.executor.factory.PropertyFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.map.HashedMap;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.formats.json.JsonRowDeserializationSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.Kafka011TableSource;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.Types;

import java.util.Map;

/**
 * 注册表信息，生成Table Source，并注册到该Session的环境中。
 */
@Slf4j
public class TableRegister {

    private Map<String, FlinkKafkaConsumer011> consumer011Map = new HashedMap(64);


    /**
     * 为session注册执行环境（订阅session制定的topic），session会向该topic中发送表信息。
     * 然后根据表的信息定制获取该表数据的source。
     */
    public void register(Session session) throws Exception {

        FlinkKafkaConsumer011 fKfkConsumer011 = genConsumer011(session);

        final StreamExecutionEnvironment sEnv = StreamExecutionEnvironment.createLocalEnvironment();
        session.setSEnv(sEnv);

        sEnv.addSource(fKfkConsumer011).map(new MapFunction<String, String>() {
            public String map(String value) throws Exception {

                Tbl tbl = JSON.parseObject(value, Tbl.class);
                log.info("Table info: {} {}", tbl.getSchema(), tbl.getTblName());

                String[] fieldNames = (String[]) tbl.getFiledNameList().toArray();
                String[] fieldTypes = (String[]) tbl.getFiledTypeList().toArray();

                TypeInformation[] typeInformations = judgeTypes(fieldTypes);
                JsonRowDeserializationSchema schema = new JsonRowDeserializationSchema(Types.ROW(fieldNames, typeInformations));

                Kafka011TableSource source = new Kafka011TableSource(new TableSchema(fieldNames, typeInformations)
                        , tbl.getTblName()
                        , PropertyFactory.genKfkProperties()
                        , schema);

                session.getExeCtx().getSourceList().put(tbl.getTblName(), source);

                return null;
            }
        });

        sEnv.execute();
    }

    private TypeInformation[] judgeTypes(String[] fieldTypes) {
        TypeInformation[] informations = new TypeInformation[fieldTypes.length];

        for (int i = 0; i < fieldTypes.length; i++) {
            switch (fieldTypes[i].toLowerCase()) {
                case "int":
                    informations[i] = Types.INT();
                    break;
                case "long":
                    informations[i] = Types.LONG();
                    break;
                case "float":
                    informations[i] = Types.FLOAT();
                    break;
                case "double":
                    informations[i] = Types.DOUBLE();
                    break;
                case "byte":
                    informations[i] = Types.BYTE();
                    break;
                case "string":
                    informations[i] = Types.STRING();
                    break;

                default:
                    throw new RuntimeException("尚未支持该数据类型: " + fieldTypes[i]);

            }
        }
        return informations;
    }

    private FlinkKafkaConsumer011 genConsumer011(Session session) {
        FlinkKafkaConsumer011 consumer011 = this.consumer011Map.get(
                session.getToken() + ":" + session.getTopic());

        if (null == consumer011) {
            consumer011 = new FlinkKafkaConsumer011(session.getTopic(),
                    new SimpleStringSchema(), PropertyFactory.genKfkProperties());
            this.consumer011Map.put(session.getTopic() + ":" + session.getTopic(), consumer011);
        }

        return consumer011;
    }

}
