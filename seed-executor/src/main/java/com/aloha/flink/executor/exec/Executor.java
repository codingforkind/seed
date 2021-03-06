package com.aloha.flink.executor.exec;


import com.alibaba.fastjson.JSON;
import com.aloha.flink.common.protocol.Tbl;
import com.aloha.flink.executor.context.session.Session;
import com.aloha.flink.executor.factory.PropertyFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.map.HashedMap;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.formats.json.JsonRowDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.Kafka011TableSource;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * 注册表信息，生成Table Source，并注册到该Session的环境中。
 */
@Slf4j
public class Executor {

    private Map<String, FlinkKafkaConsumer011> consumer011Map = new HashedMap(64);


    /**
     * 为session注册执行环境（订阅session制定的topic），session会向该topic中发送表信息。
     * 然后根据表的信息定制获取该表数据的source。
     */
    public void register(Session session) throws Exception {

        FlinkKafkaConsumer011 fKfkConsumer011 = genConsumer011(session.getToken(), session.getTblSrcTopic());

        final StreamExecutionEnvironment sEnv = StreamExecutionEnvironment.createLocalEnvironment();

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

                session.getExeCtx().getSourceMap().put(tbl.getTblName(), source);

                return null;
            }
        });

        sEnv.execute();
    }

    /**
     * 执行Session中的sql
     * 每当session中有sql到来时，就会执行sql。
     * // TODO 待优化，仅注册该sql中含有的表作为数据源
     */
    public void execute(Session session) throws Exception {
        FlinkKafkaConsumer011 fKfkConsumer011 = genConsumer011(session.getToken(), session.getExeSqlTopic());
        final StreamExecutionEnvironment sEnv = StreamExecutionEnvironment.createLocalEnvironment();

        sEnv.addSource(fKfkConsumer011).flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {

                StreamTableEnvironment tEnv = StreamTableEnvironment.create(sEnv);
                Map<String, Kafka011TableSource> sourceMap = session.getExeCtx().getSourceMap();

                Set<Map.Entry<String, Kafka011TableSource>> entries = sourceMap.entrySet();
                Iterator<Map.Entry<String, Kafka011TableSource>> iterator = entries.iterator();
                while (iterator.hasNext()) {
                    Map.Entry<String, Kafka011TableSource> next = iterator.next();
                    String tblName = next.getKey();
                    Kafka011TableSource tblSrc = next.getValue();

                    tEnv.registerTableSource(tblName, tblSrc);
                }

                log.info("SQL: []", value);

                Table rst = tEnv.sqlQuery(value);
                DataStream<Tuple2<Boolean, Row>> rstDS = tEnv.toRetractStream(rst, Row.class);
                // write result to sink


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

    public FlinkKafkaConsumer011 genConsumer011(String token, String topic) {
        FlinkKafkaConsumer011 consumer011 = this.consumer011Map.get(token + ":" + topic);

        if (null == consumer011) {
            consumer011 = new FlinkKafkaConsumer011(topic
                    , new SimpleStringSchema()
                    , PropertyFactory.genKfkProperties());
            this.consumer011Map.put(token + ":" + topic, consumer011);
        }

        return consumer011;
    }
}
