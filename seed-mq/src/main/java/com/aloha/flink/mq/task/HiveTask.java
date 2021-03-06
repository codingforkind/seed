package com.aloha.flink.mq.task;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.StringTokenizer;

public class HiveTask {
    private static final Logger LOG = LoggerFactory.getLogger(HiveTask.class);


    public static class HiveMapper extends Mapper<Object, Text, Text, IntWritable> {
        //<KEYIN, VALUEIN, KEYOUT, VALUEOUT>

        private final IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());

            while (itr.hasMoreTokens()) {
                String s = itr.nextToken();
                LOG.info("COL: {}", s);

                word.set(s);
                context.write(word, one);
            }
        }
    }

    public static class HiveReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        // <KEYIN, VALUEIN, KEYOUT, VALUEOUT>

        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context
        ) throws IOException, InterruptedException {
//            int sum = 0;
//            for (IntWritable val : values) {
//                sum += val.get();
//            }
//            result.set(sum);
//            context.write(key, result);
        }
    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

//        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
//        if (otherArgs.length < 2) {
//            System.err.println("Usage: wordcount <in> [<in>...] <out>");
//            System.exit(2);
//        }
        Job job = Job.getInstance(conf, "Hive reader");
        job.setJarByClass(HiveTask.class);
        job.setMapperClass(HiveMapper.class);
        job.setCombinerClass(HiveReducer.class);
        job.setReducerClass(HiveReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        String in = "hdfs://master:9000/study/word_count";
//        FileInputFormat.addInputPath(job, new Path(in));
        TextInputFormat.addInputPath(job, new Path(in));


        String out = "hdfs://master:9000/study/result/char_rst";
        Path path = new Path(out);
        FileSystem fileSystem = path.getFileSystem(conf);
        boolean delete = fileSystem.delete(path, true); // 文件夹不存在也可以删除，只不过返回值是false
        FileOutputFormat.setOutputPath(job, path);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
