package com.basic.spark.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;

import java.util.Arrays;
import java.util.List;

/**
 * locate com.basic.spark.core
 * Created by 79875 on 2017/10/24.
 * 广播变量 每台机器节点中有一份广播变量数据
 *
 */
public class BroadCastValue {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("BroadCastValue")
                .setMaster("local");
        final JavaSparkContext sc = new JavaSparkContext(conf);
        List<Integer> numbers= Arrays.asList(1,2,3,4,5);
        JavaRDD<Integer> numbersRDD=sc.parallelize(numbers);
        final int f=3;
        //广播变量
        final Broadcast<Integer> broadCatsValue=sc.broadcast(f);

        JavaRDD<Integer> mapRDD = numbersRDD.map(new Function<Integer, Integer>() {
            @Override
            public Integer call(Integer v1) throws Exception {
                //只读的
                return v1 * broadCatsValue.value();
            }
        });

        mapRDD.foreach(new VoidFunction<Integer>() {
            @Override
            public void call(Integer integer) throws Exception {
                System.out.println(integer);
            }
        });

        sc.close();
    }
}
