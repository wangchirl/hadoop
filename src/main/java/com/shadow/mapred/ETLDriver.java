package com.shadow.mapred;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 数据清洗：Driver
 * 抽取（Extract） -> 转换（Transform） -> 加载（Load）
 * <p>
 * hadoop jar wc.jar com.shadow.mapred.ETLDriver /usr/root/etl/input /usr/root/etl/output
 */
public class ETLDriver {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(ETLDriver.class);

        job.setMapperClass(ETLMapper.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        // TODO 设置 reduceTask 个数为 0
        job.setNumReduceTasks(0);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }
}

class ETLMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 1、获取一行数据
        String line = value.toString();
        // 2、解析日志,符合要求的写出
        if (parseLog(line, context)) {
            context.write(value, NullWritable.get());
        }
    }

    private boolean parseLog(String line, Context context) {
        // 1、截取判断长度
        String[] fields = line.split(" ");
        return fields.length > 11;
    }
}
