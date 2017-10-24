package com.basic.spark.operator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.VoidFunction;

import java.util.*;

/**
 * locate com.basic.spark.operator
 * Created by 79875 on 2017/10/23.
 * RDD MapPartition操作算子
 * 不是shuffle算子
 */
public class MapPartitionOperator {
    public static void main(String[] args) {
        SparkConf conf=new SparkConf().setAppName("MapPartitionOperator")
                .setMaster("local[2]");
        conf.set("spark.default.parallelism","2");//项目中一般设置默认并行度 shuffle操作算子后生成的RDD默认分区个数
        JavaSparkContext sc=new JavaSparkContext(conf);

        //准备一下数据
        List<String> names= Arrays.asList("tanjie","zhangfan","lincangfu");
        JavaRDD<String> nameRDD=sc.parallelize(names);

        final Map<String,Integer> scoreMap=new HashMap<>();
        scoreMap.put("tanjie",100);
        scoreMap.put("zhangfan",90);
        scoreMap.put("lincangfu",80);

        //mapPartitions
        //map算子，一次就处理一个partitions的一条数据！！！
        // mapPartitons算子，一次处理一个partition中所有数据！！！

        //推荐的使用场景
        //如果你的RDD数据不是特别多，难免采用MapPartitions算子代替map算子，可以加快处理速度
        //比如说100亿条数据，你一个parition里面就有10亿条数据，不建议使用mappartitions，
        //内存溢出

        JavaRDD<Integer> integerJavaRDD = nameRDD.mapPartitions(new FlatMapFunction<Iterator<String>, Integer>() {
            @Override
            public Iterable<Integer> call(Iterator<String> stringIterator) throws Exception {
                List<Integer> list = new ArrayList<>();
                while (stringIterator.hasNext()) {
                    String name = stringIterator.next();
                    Integer score = scoreMap.get(name);
                    list.add(score);
                }
                return list;
            }
        });

        integerJavaRDD.foreach(new VoidFunction<Integer>() {
            @Override
            public void call(Integer integer) throws Exception {
                System.out.println(integer);
            }
        });
    }
}
