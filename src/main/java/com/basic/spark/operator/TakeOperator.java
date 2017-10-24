package com.basic.spark.operator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

/**
 * locate com.basic.spark.operator
 * Created by 79875 on 2017/10/24.
 * RDD Take操作算子
 * Action操作
 */
public class TakeOperator {
    public static void main(String[] args) {
        SparkConf conf=new SparkConf().setAppName("MapOperator")
                .setMaster("local");
        conf.set("spark.default.parallelism","2");
        JavaSparkContext sc=new JavaSparkContext(conf);

        List<Integer> numbers= Arrays.asList(1,2,3,4,5);
        JavaRDD<Integer> numbersRDD=sc.parallelize(numbers);

        List<Integer> takeList = numbersRDD.take(2);
        for(Integer integer:takeList){
            System.out.println(integer);
        }
        sc.close();
    }
}
