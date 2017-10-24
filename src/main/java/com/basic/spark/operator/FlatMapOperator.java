package com.basic.spark.operator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.VoidFunction;

import java.util.Arrays;
import java.util.List;

/**
 * locate com.basic.spark.operator
 * Created by 79875 on 2017/10/24.
 * FlatMap 算子操作
 * 不是shuffle算子
 */
public class FlatMapOperator {
    public static void main(String[] args) {
        SparkConf conf=new SparkConf().setAppName("FlatMapOperator")
                .setMaster("local[2]");
        conf.set("spark.default.parallelism","2");//项目中一般设置默认并行度 shuffle操作算子后生成的RDD默认分区个数
        JavaSparkContext sc=new JavaSparkContext(conf);

        //准备一下数据
        List<String> names= Arrays.asList("hello tanjie","hello zhangfan","hello lincangfu");
        JavaRDD<String> parallelizeRDD = sc.parallelize(names);
        JavaRDD<String> flatMapRDD = parallelizeRDD.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterable<String> call(String s) throws Exception {
                return Arrays.asList(s.split(" "));
            }
        });

        flatMapRDD.foreach(new VoidFunction<String>() {
            @Override
            public void call(String s) throws Exception {
                System.out.println(s);
            }
        });

        sc.close();
    }
}
