package com.basic.spark.operator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

/**
 * locate com.basic.spark.operator
 * Created by 79875 on 2017/10/24.
 *  RDD AggerateByKey操作算子
 * 是shuffle算子
 */
public class AggerateByKeyOperator {
    public static void main(String[] args) {
        SparkConf conf=new SparkConf().setAppName("AggerateByKeyOperator")
                .setMaster("local[2]");
        conf.set("spark.default.parallelism","2");
        JavaSparkContext sc=new JavaSparkContext(conf);

        List<String> names= Arrays.asList("tanjie is a good gay","zhangfan is a good gay","lincangfu is a good gay","tanjie","lincangfu");
        JavaRDD<String> nameRDD=sc.parallelize(names);

        JavaRDD<String> wordsRDD = nameRDD.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterable<String> call(String s) throws Exception {
                String[] split = s.split(" ");
                return Arrays.asList(split);
            }
        });
        JavaPairRDD<String, Integer> wordsPairRDD = wordsRDD.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<>(s, 1);
            }
        });

        // aggregateByKey操作其实这个和reduceByKey操作差不多，reduceByKey是aggregateByKey简化版
        // aggregateByKey里面参数需要三个
        // 第一个参数，每个Key的初始值
        // 第二个参数，Seq Function,是如何进行Shuffle map-side的本地聚合
        // 第三个参数，说白了就是如何进行Shuffle reduce-side的全局聚合

        //reduce foldLeft

        JavaPairRDD<String, Integer> aggregateRDD = wordsPairRDD.aggregateByKey(0, new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        }, new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        aggregateRDD.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            @Override
            public void call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                System.out.println(stringIntegerTuple2._1+" "+stringIntegerTuple2._2);
            }
        });

        sc.close();
    }
}
