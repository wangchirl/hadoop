package com.shadow.mapred;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 单词统计：Driver
 * <p>
 * Reduce阶段统计汇总：hadoop jar wc.jar com.shadow.mapred.WordCountDriver /usr/root/wc/input /usr/root/wc/output1 false
 * Map阶段统计汇总：hadoop jar wc.jar com.shadow.mapred.WordCountDriver /usr/root/wc/input /usr/root/wc/output2 true
 */
public class WordCountDriver {

    public static void main(String[] args) throws Exception {
        // 1、获取配置信息以及获取job对象
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        // 2、关联本 Driver 程序的 jar
        job.setJarByClass(WordCountDriver.class);

        // 3、关联Mapper及Reducer的 jar
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        // TODO 设置 Combiner，逻辑与 WordCountReducer 类似，运行位置不一样
        if (Boolean.parseBoolean(args[2])) {
            job.setCombinerClass(WordCountCombiner.class);
        }

        // 4、设置 Mapper 的输出KV类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // 5、设置最终输出的 KV 类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // 6、设置输入和输出路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 7、提交任务
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }
}

/**
 * Combiner
 * <p>
 * 运行在 Driver 端
 */
class WordCountCombiner extends Reducer<Text, IntWritable, Text, IntWritable> {

    private IntWritable v = new IntWritable();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable value : values) {
            sum += value.get();
        }
        v.set(sum);
        context.write(key, v);
    }
}

/**
 * Mapper
 */
class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private Text k = new Text();
    private IntWritable v = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 1、获取一行
        String line = value.toString();
        // 2、分割行
        // atguigu atguigu
        String[] words = line.split(" ");
        // 3、写出
        for (String word : words) {
            k.set(word);
            context.write(k, v);
        }
    }
}

/**
 * Reducer
 */
class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    private IntWritable v = new IntWritable();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        // 1、求累加和
        int sum = 0;
        for (IntWritable value : values) {
            sum += value.get();
        }
        // 2、写出
        v.set(sum);
        context.write(key, v);
    }
}