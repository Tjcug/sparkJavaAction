package com.basic.spark;

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
 * locate com.basic.spark
 * Created by 79875 on 2017/10/23.
 * Spark 实现WordCount程序
 */
public class ParallelizeWordCount {
    public static void main(String[] args) {
        SparkConf conf=new SparkConf().setAppName("ParallelizeWordCount")
                .setMaster("local");
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

        JavaPairRDD<String, Integer> wordcountRDD = wordsPairRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        wordcountRDD.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            @Override
            public void call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                System.out.println(stringIntegerTuple2);
            }
        });

        sc.close();
    }
}
