package com.shadow.mapred;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 流量统计：Driver
 * <p>
 * 使用默认分区器：hadoop jar wc.jar com.shadow.mapred.FlowDriver /usr/root/flow/input1 /usr/root/flow/output1 false
 * 使用自定义分区器：hadoop jar wc.jar com.shadow.mapred.FlowDriver /usr/root/flow/input1 /usr/root/flow/output2 true
 */
public class FlowDriver {
    public static void main(String[] args) throws Exception {
        // 1、获取job
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        // 2、设置Driver类
        job.setJarByClass(FlowDriver.class);

        // 3、设置Mapper及Reducer
        job.setMapperClass(FlowMapper.class);
        job.setReducerClass(FlowReducer.class);

        // 4、设置Mapper输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);

        // 5、设置最终输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        // TODO 自定义分区器,默认为 HashPartitioner,同时需要知道相应的数量的 ReduceTask
        if (Boolean.parseBoolean(args[2])) {
            job.setPartitionerClass(ProvincePartitioner.class);
            job.setNumReduceTasks(5);
        }

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
class FlowBean implements Writable {
    private long upFlow;
    private long downFlow;
    private long sumFlow;

    public FlowBean() {
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
 * 自定义分区器
 */
class ProvincePartitioner extends Partitioner<Text, FlowBean> {
    @Override
    public int getPartition(Text text, FlowBean flowBean, int numPartitions) {
        // 1、获取记录手机号前三位
        String prePhone = text.toString().substring(0, 3);
        // 2、定义分区号变量
        int partition;
        if ("136".equals(prePhone)) {
            partition = 0;
        } else if ("137".equals(prePhone)) {
            partition = 1;
        } else if ("138".equals(prePhone)) {
            partition = 2;
        } else if ("139".equals(prePhone)) {
            partition = 3;
        } else {
            partition = 4;
        }
        return partition;
    }
}



/**
 * Mapper
 */
class FlowMapper extends Mapper<LongWritable, Text, Text, FlowBean> {

    private Text k = new Text();
    private FlowBean v = new FlowBean();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 1、获取一行数据，转为字符串
        // 1	13736230513	192.196.100.1	www.atguigu.com	2481	24681	200
        String line = value.toString();
        // 2、分割数据
        String[] split = line.split("\t");
        // 3、获取需要的内容
        String phone = split[1];
        String up = split[split.length - 3];
        String down = split[split.length - 2];
        // 4、封装KV
        k.set(phone);
        v.setUpFlow(Long.parseLong(up));
        v.setDownFlow(Long.parseLong(down));
        v.setSumFlow();
        // 5、输出
        context.write(k, v);
    }
}

/**
 * Reducer
 */
class FlowReducer extends Reducer<Text, FlowBean, Text, FlowBean> {

    private FlowBean v = new FlowBean();

    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
        // 1、遍历相同key的values，累加流量
        long totalUp = 0;
        long totalDown = 0;
        for (FlowBean value : values) {
            totalUp += value.getUpFlow();
            totalDown += value.getDownFlow();
        }
        // 2、封装V
        v.setUpFlow(totalUp);
        v.setDownFlow(totalDown);
        v.setSumFlow();
        // 3、输出
        context.write(key, v);
    }
}
