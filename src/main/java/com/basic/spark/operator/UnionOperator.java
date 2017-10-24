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
 * RDD Union操作算子
 * 合并操作
 * 不是shuffle算子
 */
public class UnionOperator {
    public static void main(String[] args) {
        SparkConf conf=new SparkConf().setAppName("UnionOperator")
                .setMaster("local[2]");
        conf.set("spark.default.parallelism","2");//项目中一般设置默认并行度
        JavaSparkContext sc=new JavaSparkContext(conf);

        //准备一下数据
        List<String> names1= Arrays.asList("tanjie","zhangfan","lincangfu","haotongbao");
        JavaRDD<String> name1RDD=sc.parallelize(names1);

        List<String> names2= Arrays.asList("tanjie","zhangfan","lincangfu","zhangwangcheng");
        JavaRDD<String> name2RDD=sc.parallelize(names2);

        JavaRDD<String> unionRDD = name1RDD.union(name2RDD);
        unionRDD.foreach(new VoidFunction<String>() {
            @Override
            public void call(String s) throws Exception {
                System.out.println(s);
            }
        });
        System.out.println(unionRDD.splits().size());
        sc.close();
    }
}
