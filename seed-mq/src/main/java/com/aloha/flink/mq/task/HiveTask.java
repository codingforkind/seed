package com.aloha.flink.mq.task;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class HiveTask {

    class HiveMapper extends Mapper { //<KEYIN, VALUEIN, KEYOUT, VALUEOUT>

        @Override
        protected void map(Object key, Object value, Context context) throws IOException, InterruptedException {
            super.map(key, value, context);
        }
    }

    class HiveReducer extends Reducer { // <KEYIN, VALUEIN, KEYOUT, VALUEOUT>

        @Override
        protected void reduce(Object key, Iterable values, Context context) throws IOException, InterruptedException {
            super.reduce(key, values, context);
        }
    }
}
