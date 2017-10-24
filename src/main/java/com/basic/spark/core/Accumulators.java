package com.basic.spark.core;

import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

import java.util.Arrays;
import java.util.List;

/**
 * locate com.basic.spark.core
 * Created by 79875 on 2017/10/24.
 * 累加器  所有机器共享一份Accumulators累加器
 * 可以实时看到累加器的结果，可以再SparkUI中看到累加器
 * 集群Task在运行的时候只能add或者累加器，不能读取累加器的数据，因为累加器是放在Driver端中
 * Driver端可以读取累加器的值
 */
public class Accumulators {
    public static void main(String[] args) throws InterruptedException {
        SparkConf conf = new SparkConf().setAppName("Accumulators")
                .setMaster("local");
        final JavaSparkContext sc = new JavaSparkContext(conf);
        List<Integer> numbers= Arrays.asList(1,2,3,4,5);
        JavaRDD<Integer> numbersRDD=sc.parallelize(numbers);

        final Accumulator<Integer> accumulator = sc.accumulator(0,"MyAccumulator");

        numbersRDD.foreach(new VoidFunction<Integer>() {
            @Override
            public void call(Integer integer) throws Exception {
                accumulator.add(integer);
            }
        });
        System.out.println(accumulator.value());
        Thread.sleep(60*1000*60);
        sc.close();
    }
}
