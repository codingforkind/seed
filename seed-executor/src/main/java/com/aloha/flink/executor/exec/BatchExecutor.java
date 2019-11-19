package com.aloha.flink.executor.exec;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Arrays;
import java.util.List;
import java.util.StringTokenizer;

@Slf4j
public class BatchExecutor {

    public static void main(String[] args) {


        String test = "one 1\n" +
                "two 2\n" +
                "three 3\n" +
                "four 4\n" +
                "five 5\n" +
                "six 6\n" +
                "seven 7\n" +
                "eight 8\n" +
                "nine 9\n" +
                "ten 10";
        StringTokenizer itr = new StringTokenizer(test);

        while (itr.hasMoreTokens()) {
            System.out.println(itr.nextToken());
        }

//
////        String url = "hdfs://master:9000/study/char_count";
//        String url = "hdfs://master:9000/study/word_count";
//        ExecutionEnvironment exeEnv = ExecutionEnvironment.getExecutionEnvironment();
//        BatchExecutor executor = new BatchExecutor();
//
////        AggregateOperator<Tuple2<String, Integer>> sum
//////                = executor.getSum1(exeEnv, url);
////                = executor.getSum2(exeEnv, url);
////        try {
////            sum.print();
////        } catch (Exception e) {
////            e.printStackTrace();
////        }
//
//        executor.test(exeEnv, url);
//
//        try {
//            exeEnv.execute("Read hdfs testing.");
//        } catch (
//                Exception e) {
//            e.printStackTrace();
//        }
    }


    private void test(ExecutionEnvironment exeEnv, String url) {

        List<String> collect = null;

        try {
            collect = exeEnv.readTextFile(url)
                    .map(new MapFunction<String, String[]>() {
                        @Override
                        public String[] map(String value) throws Exception {
                            return value.split("\\n");
                        }
                    })
                    .flatMap(new FlatMapFunction<String[], String>() {
                        @Override
                        public void flatMap(String[] value, Collector<String> out) throws Exception {
                            out.collect(value[0]);
                        }
                    })
                    .collect();
        } catch (Exception e) {
            e.printStackTrace();
        }

        for (String tmp : collect) {
            System.out.println(tmp);
        }


    }


    private AggregateOperator<Tuple2<String, Integer>> getSum2(ExecutionEnvironment exeEnv, String url) {

        return exeEnv.readTextFile(url)
                .map(new MapFunction<String, String[]>() {
                    @Override
                    public String[] map(String value) throws Exception {
                        return value.split("\\W+");
                    }
                })
                .flatMap(new FlatMapFunction<String[], Tuple2<String, Integer>>() {
                    @Override
                    public void flatMap(String[] value, Collector<Tuple2<String, Integer>> out) throws Exception {
//                        Arrays.stream(value).forEach(key -> out.collect(new Tuple2<>(key, 1)));

                        for (String key : value) {
                            if (key.length() > 0) {
                                out.collect(new Tuple2<>(key, 1));
                            }
                        }
                    }
                })
                .groupBy(0)
                .sum(1);


    }

    private AggregateOperator<Tuple2<String, Integer>> getSum1(ExecutionEnvironment exeEnv, String url) {

        return exeEnv.readTextFile(url)
                .flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
                    /** 注意
                     * 1. FlatMapFunction中处理的入参时String，这个String就是数据流中的一个元素
                     * 2. FlatMapFunction的入参类型（当前是String），取决于调用flatMap方法的实例所包含的元素的类型。
                     * 本例中，readTextFile方法返回的数据的类型是String类型，那么flatMap算子的入参是String类型。
                     */

                    @Override
                    public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
//                        Arrays.stream(value).forEach(key -> out.collect(new Tuple2<>(key, 1)));
                        String[] split = value.split("\\W+");

                        for (String key : split) {
                            if (key.length() > 0) {
                                out.collect(new Tuple2<>(key, 1));
                            }
                        }
                    }
                })
                .groupBy(0)
                .sum(1);

    }
}
