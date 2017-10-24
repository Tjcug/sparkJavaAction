package com.basic.spark.operator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;

import java.util.Arrays;
import java.util.List;

/**
 * locate com.basic.spark.operator
 * Created by 79875 on 2017/10/24.
 * Collect 算子操作 Action操作
 */
public class CollectOperator {
    public static void main(String[] args) {
        SparkConf conf=new SparkConf().setAppName("MapOperator")
                .setMaster("local[2]");
        conf.set("spark.default.parallelism","2");//项目中一般设置默认并行度 shuffle操作算子后生成的RDD默认分区个数
        JavaSparkContext sc=new JavaSparkContext(conf);

        List<Integer> numbers= Arrays.asList(1,2,3,4,5);
        JavaRDD<Integer> numbersRDD=sc.parallelize(numbers);

        //map对每个元素进行操作
        JavaRDD<Integer> mapRDD = numbersRDD.map(new Function<Integer, Integer>() {
            @Override
            public Integer call(Integer v1) throws Exception {
                return v1 * 2;
            }
        });

        mapRDD.foreach(new VoidFunction<Integer>() {
            @Override
            public void call(Integer integer) throws Exception {
                System.out.println(integer);
            }
        });

        //用foreach action操作，collect在远程集群上遍历RDD的元素
        // 用collect操作，将分布式的在远程集群里面的数据拉到本地！！
        // 这种方式不建议使用，如果数据量太大，走大量的网络传输
        // 甚至可能OOM的内存溢出，通常情况下你会看到foreach操作
        List<Integer> collect = mapRDD.collect();
        for(Integer integer:collect){
            System.out.println(integer);
        }
        sc.close();
    }
}
