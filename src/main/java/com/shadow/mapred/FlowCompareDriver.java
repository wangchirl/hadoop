package com.shadow.mapred;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 流量统计：Driver
 * <p>
 * 全排序：hadoop jar wc.jar com.shadow.mapred.FlowCompareDriver /usr/root/flowsort/input /usr/root/flowsort/output1 false
 */
public class FlowCompareDriver {

    public static void main(String[] args) throws Exception {
        // 1、获取job
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        // 2、设置Driver类
        job.setJarByClass(FlowCompareDriver.class);

        // 3、设置Mapper及Reducer
        job.setMapperClass(FlowComparableMapper.class);
        job.setReducerClass(FlowComparableReducer.class);

        // 4、设置Mapper输出类型
        job.setMapOutputKeyClass(FlowComparableBean.class);
        job.setMapOutputValueClass(Text.class);

        // 5、设置最终输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowComparableBean.class);

        // 6、设置文件输入输出路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 7、执行
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }
}

/**
 * 自定义Bean 实现 hadoop 序列化 Writable，而 WritableComparable 附加排序功能
 */
class FlowComparableBean implements WritableComparable<FlowComparableBean> {
    private long upFlow;
    private long downFlow;
    private long sumFlow;

    public FlowComparableBean() {
        super();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(upFlow);
        out.writeLong(downFlow);
        out.writeLong(sumFlow);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        upFlow = in.readLong();
        downFlow = in.readLong();
        sumFlow = in.readLong();
    }

    /**
     * 降序
     */
    @Override
    public int compareTo(FlowComparableBean o) {
        if (this.sumFlow > o.sumFlow) {
            return -1;
        } else if (this.sumFlow < o.sumFlow) {
            return 1;
        } else {
            return 0;
        }
    }

    @Override
    public String toString() {
        return upFlow + "\t" + downFlow + "\t" + sumFlow;
    }

    public long getUpFlow() {
        return upFlow;
    }

    public void setUpFlow(long upFlow) {
        this.upFlow = upFlow;
    }

    public long getDownFlow() {
        return downFlow;
    }

    public void setDownFlow(long downFlow) {
        this.downFlow = downFlow;
    }

    public long getSumFlow() {
        return sumFlow;
    }

    public void setSumFlow() {
        this.sumFlow = this.upFlow + this.downFlow;
    }
}

/**
 * Mapper
 */
class FlowComparableMapper extends Mapper<LongWritable, Text, FlowComparableBean, Text> {

    private FlowComparableBean k = new FlowComparableBean();
    private Text v = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 1、获取一行数据
        String line = value.toString();
        // 2、分割 1	 13736230513	192.196.100.1	www.atguigu.com	2481	24681	200
        String[] split = line.split("\t");
        // 3、封装
        k.setUpFlow(Long.parseLong(split[split.length - 3]));
        k.setDownFlow(Long.parseLong(split[split.length - 2]));
        k.setSumFlow();
        v.set(split[1]);
        // 4、写出
        context.write(k, v);
    }
}

/**
 * Reducer
 */
class FlowComparableReducer extends Reducer<FlowComparableBean, Text, Text, FlowComparableBean> {

    @Override
    protected void reduce(FlowComparableBean key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text value : values) {
            context.write(value, key);
        }
    }
}