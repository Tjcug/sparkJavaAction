package com.basic.spark.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.List;

/**
 * locate com.basic.spark.core
 * Created by 79875 on 2017/10/24.
 * TOPN 问题
 */
public class TOPN {
    public static void main(String[] args) {
        SparkConf conf=new SparkConf().setAppName("TOPN")
                .setMaster("local");
        JavaSparkContext sc=new JavaSparkContext(conf);

        JavaRDD<String> textFileRDD = sc.textFile("data/top.txt");
        JavaPairRDD<Integer, String> mapToPairRDD = textFileRDD.mapToPair(new PairFunction<String, Integer, String>() {
            @Override
            public Tuple2<Integer, String> call(String s) throws Exception {
                return new Tuple2<>(Integer.valueOf(s), s);
            }
        });

        JavaPairRDD<Integer, String> sortByKeyRDD = mapToPairRDD.sortByKey(false);
        JavaRDD<String> mapRDD = sortByKeyRDD.map(new Function<Tuple2<Integer, String>, String>() {
            @Override
            public String call(Tuple2<Integer, String> v1) throws Exception {
                return v1._2;
            }
        });
        List<String> takeRDD = mapRDD.take(3);
        for(String str:takeRDD){
            System.out.println(str);
        }

        sc.close();
    }
}
