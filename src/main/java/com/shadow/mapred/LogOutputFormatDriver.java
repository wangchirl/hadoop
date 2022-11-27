package com.shadow.mapred;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 自定义输出：Driver
 * <p>
 * hadoop jar wc.jar com.shadow.mapred.LogOutputFormatDriver /usr/root/log/input /usr/root/log/output
 */
public class LogOutputFormatDriver {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(LogOutputFormatDriver.class);

        job.setMapperClass(LogOutputFormatMapper.class);
        job.setReducerClass(LogOutputFormatReducer.class);

        // TODO 设置自定义 OutputFormat
        job.setOutputFormatClass(LogOutputFormat.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        // 虽然我们自定义了outputformat，但是因为我们的outputformat继承自fileoutputformat
        // 而fileoutputformat要输出一个_SUCCESS文件，所以在这还得指定一个输出目录
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }
}

/**
 * FileOutputFormat
 */
class LogOutputFormat extends FileOutputFormat<Text, NullWritable> {
    @Override
    public RecordWriter<Text, NullWritable> getRecordWriter(TaskAttemptContext job) throws IOException, InterruptedException {
        return new LogOutputFormatRecordWriter(job);
    }
}

/**
 * RecordWriter
 */
class LogOutputFormatRecordWriter extends RecordWriter<Text, NullWritable> {

    private FSDataOutputStream targetOut;
    private FSDataOutputStream otherOut;

    public LogOutputFormatRecordWriter(TaskAttemptContext job) {
        try {
            // 1、获取文件系统
            FileSystem fs = FileSystem.get(job.getConfiguration());
            // 2、用文件系统创建输出流到不同目录
            targetOut = fs.create(new Path("/usr/root/outputlog/target.log"));
            otherOut = fs.create(new Path("/usr/root/outputlog/other.log"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void write(Text key, NullWritable value) throws IOException, InterruptedException {
        String log = key.toString();
        if (log.contains("atguigu")) {
            targetOut.writeBytes(log + " \t");
        } else {
            otherOut.writeBytes(log + "\t");
        }
    }

    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {
        targetOut.close();
        otherOut.close();
    }
}

/**
 * Mapper
 */
class LogOutputFormatMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        context.write(value, NullWritable.get());
    }
}

/**
 * Reducer
 */
class LogOutputFormatReducer extends Reducer<Text, NullWritable, Text, NullWritable> {

    @Override
    protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        for (NullWritable value : values) {
            context.write(key, NullWritable.get());
        }
    }
}
