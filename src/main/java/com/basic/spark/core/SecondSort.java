package com.basic.spark.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

/**
 * locate com.basic.spark.core
 * Created by 79875 on 2017/10/24.
 * Spark 实现二次排序
 */
public class SecondSort {
    public static void main(String[] args) {
        SparkConf conf=new SparkConf().setAppName("SecondSort")
                .setMaster("local");
        JavaSparkContext sc=new JavaSparkContext(conf);

        JavaRDD<String> textFileRDD = sc.textFile("data/secondsort.txt");
        JavaPairRDD<SecondSortKey,String > pairRDD = textFileRDD.mapToPair(new PairFunction<String, SecondSortKey, String>() {
            @Override
            public Tuple2<SecondSortKey, String> call(String s) throws Exception {
                String[] split = s.split(" ");
                return new Tuple2<>(new SecondSortKey(Integer.valueOf(split[0]), Integer.valueOf(split[2])),split[1]);
            }
        });
        JavaPairRDD<SecondSortKey, String> sortByKeyRDD = pairRDD.sortByKey(false);

        JavaRDD<String> mapRDD= sortByKeyRDD.map(new Function<Tuple2<SecondSortKey, String>, String>() {
            @Override
            public String call(Tuple2<SecondSortKey, String> v1) throws Exception {
                return v1._2;
            }
        });

        mapRDD.foreach(new VoidFunction<String>() {
            @Override
            public void call(String s) throws Exception {
                System.out.println(s);
            }
        });

        sc.close();
    }
}


