package com.shadow.mapred;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 小文件合并 CombineTextInputFormat：Driver
 * <p>
 * hadoop jar wc.jar com.shadow.mapred.WordCountCombineDriver /usr/root/comb/input /usr/root/comb/output1
 */
public class WordCountCombineDriver {

    public static void main(String[] args) throws Exception {
        // 1、获取配置信息以及获取job对象
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        // 2、关联本 Driver 程序的 jar
        job.setJarByClass(WordCountCombineDriver.class);

        // 3、关联Mapper及Reducer的 jar
        job.setMapperClass(WordCountCombineMapper.class);
        job.setReducerClass(WordCountCombineReducer.class);

        // 4、设置 Mapper 的输出KV类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // 5、设置最终输出的 KV 类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // TODO 设置 InputFormat 类型，默认为 TextInputFormat ,CombineTextInputFormat 适合处理小文件多的场景
        job.setInputFormatClass(CombineTextInputFormat.class);
        // 设置虚拟存储切片最大值 20m
        CombineTextInputFormat.setMaxInputSplitSize(job, 1024 * 1024 * 20);

        // 6、设置输入和输出路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 7、提交任务
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }
}

/**
 * Mapper
 */
class WordCountCombineMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

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
class WordCountCombineReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

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