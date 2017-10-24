package com.basic.spark.operator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

/**
 * locate com.basic.spark.operator
 * Created by 79875 on 2017/10/24.
 * CountOperator 操作算子 Action操作
 */
public class CountOpeartor {
    public static void main(String[] args) {
        SparkConf conf=new SparkConf().setAppName("MapOperator")
                .setMaster("local[2]");
        conf.set("spark.default.parallelism","2");//项目中一般设置默认并行度 shuffle操作算子后生成的RDD默认分区个数
        JavaSparkContext sc=new JavaSparkContext(conf);

        List<Integer> numbers= Arrays.asList(1,2,3,4,5);
        JavaRDD<Integer> numbersRDD=sc.parallelize(numbers);

        long count = numbersRDD.count();
        System.out.println(count);
    }
}
