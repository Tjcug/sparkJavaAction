package com.basic.spark.operator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * locate com.basic.spark.operator
 * Created by 79875 on 2017/10/23.
 * RDD MapPartitionWithIndex操作算子
 * 不是shuffle算子
 */
public class MapPartitionWithIndexOperator {
    public static void main(String[] args) {
        SparkConf conf=new SparkConf().setAppName("MapPartitionWithIndexOperator")
                .setMaster("local[2]");
        conf.set("spark.default.parallelism","2");
        JavaSparkContext sc=new JavaSparkContext(conf);

        //准备一下数据
        List<String> names= Arrays.asList("tanjie","zhangfan","lincangfu","xuruiyun");
        JavaRDD<String> nameRDD=sc.parallelize(names);
        //其实这里不写并行度2，其实它默认也是2

        //parallelize并行集合的时候，指定了并行度为2，说白了就是numPartitions也是2（local模式取决于你的local[2] 有多少个线程运行）
        //也就是我们的names会被分到两个分区里面去

        //如果我们想知道谁和谁被分到哪个分区里面去了
        //MapPartitionWithIndex 算子操作可以拿到partitions的index
        JavaRDD<String> resultRDD = nameRDD.mapPartitionsWithIndex(new Function2<Integer, Iterator<String>, Iterator<String>>() {
            @Override
            public Iterator<String> call(Integer index, Iterator<String> v2) throws Exception {
                List<String> list = new ArrayList<>();
                while (v2.hasNext()) {
                    String name = v2.next();
                    String result=name+" "+index;
                    list.add(result);
                }
                return list.iterator();
            }
        },true);

        resultRDD.foreach(new VoidFunction<String>() {
            @Override
            public void call(String string) throws Exception {
                System.out.println(string);
            }
        });
    }
}
