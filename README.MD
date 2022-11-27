### 1、单词统计

- Reduce阶段统计汇总

  ```sh
  hadoop jar wc.jar com.shadow.mapred.WordCountDriver /usr/root/wc/input /usr/root/wc/output1 false
  ```

- Map阶段统计汇总 - job.setCombinerClass

  ```sh
  hadoop jar wc.jar com.shadow.mapred.WordCountDriver /usr/root/wc/input /usr/root/wc/output2 true
  ```

### 2、流量统计 - 分区 job.setPartitionerClass

- 使用默认分区器

  ```sh
  hadoop jar wc.jar com.shadow.mapred.FlowDriver /usr/root/flow/input1 /usr/root/flow/output1 false
  ```

- 使用自定义分区器

  ```sh
  hadoop jar wc.jar com.shadow.mapred.FlowDriver /usr/root/flow/input1 /usr/root/flow/output2 true
  ```

### 3、小文件合并 - CombineTextInputFormat

```sh
hadoop jar wc.jar com.shadow.mapred.WordCountCombineDriver /usr/root/comb/input /usr/root/comb/output1
```

### 4、流量统计 - 排序 job.setPartitionerClass

- 全排序

  ```sh
  hadoop jar wc.jar com.shadow.mapred.FlowCompareDriver /usr/root/flowsort/input1 /usr/root/flowsort/output1 false
  ```

- 分区内排序

  ```sh
  hadoop jar wc.jar com.shadow.mapred.FlowCompareDriver /usr/root/flowsort/input1 /usr/root/flowsort/output2 true
  ```

### 5、自定义输出 - job.setOutputFormatClass

```sh
hadoop jar wc.jar com.shadow.mapred.LogOutputFormatDriver /usr/root/log/input /usr/root/log/output
```

### 6、关联查询

- Reduce Join

  ```sh
  hadoop jar wc.jar com.shadow.mapred.ReduceJoinDriver /usr/root/join/input /usr/root/join/output1 false
  ```

- Mapper Join -> job.addCacheFile()

  ```sh
  hadoop jar wc.jar com.shadow.mapred.ReduceJoinDriver /usr/root/join/input/order.txt /usr/root/join/output2 true
  ```

### 7、ETL - 抽取（Extract） -> 转换（Transform） -> 加载（Load）

```sh
hadoop jar wc.jar com.shadow.mapred.ETLDriver /usr/root/etl/input /usr/root/etl/output
```







