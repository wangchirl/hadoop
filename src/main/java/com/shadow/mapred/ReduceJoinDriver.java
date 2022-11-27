package com.shadow.mapred;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.*;
import java.lang.reflect.InvocationTargetException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 关联查询：Driver
 * <p>
 * Reduce Join：hadoop jar wc.jar com.shadow.mapred.ReduceJoinDriver /usr/root/join/input /usr/root/join/output1 false
 * Mapper Join：hadoop jar wc.jar com.shadow.mapred.ReduceJoinDriver /usr/root/join/input/order.txt /usr/root/join/output2 true
 */
public class ReduceJoinDriver {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(ReduceJoinDriver.class);

        if (Boolean.parseBoolean(args[2])) {
            job.setMapperClass(MapTableJoinMapper.class);

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(NullWritable.class);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(NullWritable.class);
            // TODO 加载缓存数据
            job.addCacheFile(new URI("/usr/root/reduce/join/input/pd.txt"));
            // TODO Map 端 Join 的逻辑不需要 Reduce 阶段，设置 reduceTask数量为0
            job.setNumReduceTasks(0);
        } else {
            job.setMapperClass(ReduceTableJoinMapper.class);
            job.setReducerClass(ReduceTableJoinReducer.class);

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(ReduceTableBean.class);

            job.setOutputKeyClass(ReduceTableBean.class);
            job.setOutputValueClass(NullWritable.class);
        }

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }
}

/**
 * 自定义Bean 实现 hadoop 序列化 Writable，而 WritableComparable 附加排序功能
 */
class ReduceTableBean implements Writable {

    private String id; // 订单id
    private String pid; // 产品id
    private int amount; // 产品数量
    private String pname; // 产品名称
    private String flag; // 判断是 order 还是 pd

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(id);
        out.writeUTF(pid);
        out.writeInt(amount);
        out.writeUTF(pname);
        out.writeUTF(flag);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        id = in.readUTF();
        pid = in.readUTF();
        amount = in.readInt();
        pname = in.readUTF();
        flag = in.readUTF();
    }

    @Override
    public String toString() {
        return id + "\t" + pname + "\t" + amount;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getPid() {
        return pid;
    }

    public void setPid(String pid) {
        this.pid = pid;
    }

    public int getAmount() {
        return amount;
    }

    public void setAmount(int amount) {
        this.amount = amount;
    }

    public String getPname() {
        return pname;
    }

    public void setPname(String pname) {
        this.pname = pname;
    }

    public String getFlag() {
        return flag;
    }

    public void setFlag(String flag) {
        this.flag = flag;
    }
}

/**
 * MapMapper
 */
class MapTableJoinMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

    private Map<String, String> pdMap = new HashMap<>();
    private Text text = new Text();

    // 任务开始前将 pd 数据缓存到 pdMap
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        // 1、通过缓存文件得到小标数据 pd.txt
        URI[] cacheFiles = context.getCacheFiles();
        Path path = new Path(cacheFiles[0]);
        // 2、获取文件系统对象
        FileSystem fs = FileSystem.get(context.getConfiguration());
        FSDataInputStream fis = fs.open(path);
        // 3、读取内容
        BufferedReader reader = new BufferedReader(new InputStreamReader(fis, "UTF-8"));
        String line;
        while (StringUtils.isNotEmpty(line = reader.readLine())) {
            // 分割
            String[] split = line.split("\t");
            pdMap.put(split[0], split[1]);
        }
        // 关闭流
        IOUtils.closeStream(reader);
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 1、获取一行并分割
        String[] fileds = value.toString().split("\t");
        // 2、获取pd 信息
        String pname = pdMap.get(fileds[1]);
        // 3、组装数据
        text.set(fileds[0] + "\t" + pname + "\t" + fileds[2]);
        // 4、写出
        context.write(text, NullWritable.get());
    }
}

/**
 * ReduceMapper
 */
class ReduceTableJoinMapper extends Mapper<LongWritable, Text, Text, ReduceTableBean> {

    private String filename;
    private Text k = new Text();
    private ReduceTableBean v = new ReduceTableBean();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        // 获取对应文件名称
        FileSplit inputSplit = (FileSplit) context.getInputSplit();
        filename = inputSplit.getPath().getName();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 1、获取一行
        String line = value.toString();
        // 2、判断是哪个文件
        String[] split = line.split("\t");
        if ("order".equals(filename)) {
            // id:1001	pid:01	amount:1
            // 3、封装
            k.set(split[1]);
            v.setId(split[0]);
            v.setPid(split[1]);
            v.setAmount(Integer.parseInt(split[2]));
            v.setPname("");
            v.setFlag("order");
        } else {
            // pid:01	pname:小米
            k.set(split[0]);
            v.setId("");
            v.setPid(split[0]);
            v.setAmount(0);
            v.setPname(split[1]);
            v.setFlag("pd");
        }
        // 4、写出
        context.write(k, v);
    }
}

/**
 * Reducer
 */
class ReduceTableJoinReducer extends Reducer<Text, ReduceTableBean, ReduceTableBean, NullWritable> {
    @Override
    protected void reduce(Text key, Iterable<ReduceTableBean> values, Context context) throws IOException, InterruptedException {
        // 1、分组进来的，一批相同的 pid，order有多个进来，pd只有一个
        List<ReduceTableBean> orders = new ArrayList<>();
        ReduceTableBean pd = new ReduceTableBean();

        for (ReduceTableBean value : values) {
            // 判断来自那张表
            if ("order".equals(value.getFlag())) {
                ReduceTableBean tmp = new ReduceTableBean();
                try {
                    BeanUtils.copyProperties(tmp, value);
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                } catch (InvocationTargetException e) {
                    e.printStackTrace();
                }
                orders.add(tmp);
            } else {
                try {
                    BeanUtils.copyProperties(pd, value);
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                } catch (InvocationTargetException e) {
                    e.printStackTrace();
                }
            }
        }
        // 2、处理每个订单的 pname
        for (ReduceTableBean order : orders) {
            order.setPname(pd.getPname());
            // 3、写出
            context.write(order, NullWritable.get());
        }
    }
}
