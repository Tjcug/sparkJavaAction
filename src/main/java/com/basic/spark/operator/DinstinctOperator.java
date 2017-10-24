package com.basic.spark.operator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

import java.util.Arrays;
import java.util.List;

/**
 * locate com.basic.spark.operator
 * Created by 79875 on 2017/10/24.
 *  RDD Dinstinct操作算子
 * 去重操作
 * 是shuffle算子
 */
public class DinstinctOperator {
    public static void main(String[] args) {
        SparkConf conf=new SparkConf().setAppName("DinstinctOperator")
                .setMaster("local[2]");
        conf.set("spark.default.parallelism","2");//项目中一般设置默认并行度 shuffle操作算子后生成的RDD默认分区个数
        JavaSparkContext sc=new JavaSparkContext(conf);

        //准备一下数据
        List<String> names= Arrays.asList("tanjie","zhangfan","lincangfu","tanjie","lincangfu");
        JavaRDD<String> nameRDD=sc.parallelize(names);

        JavaRDD<String> distinctRDD = nameRDD.distinct();
        distinctRDD.foreach(new VoidFunction<String>() {
            @Override
            public void call(String s) throws Exception {
                System.out.println(s);
            }
        });
        sc.close();
    }
}
