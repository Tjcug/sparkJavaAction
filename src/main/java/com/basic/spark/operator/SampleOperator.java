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
 * 随机采样算子
 * RDD Sample操作算子
 */
public class SampleOperator {
    public static void main(String[] args) {
        SparkConf conf=new SparkConf().setAppName("MapPartitionWithIndexOperator")
                .setMaster("local[2]");
        conf.set("spark.default.parallelism","2");
        JavaSparkContext sc=new JavaSparkContext(conf);

        //准备一下数据
        List<String> names= Arrays.asList("tanjie","zhangfan","lincangfu","xuruiyun");
        JavaRDD<String> nameRDD=sc.parallelize(names);
        //第一个参数：是否有Replacement 放回去
        nameRDD.sample(true,0.5).foreach(new VoidFunction<String>() {
            @Override
            public void call(String s) throws Exception {
                System.out.println(s);
            }
        });

        sc.close();
    }
}
