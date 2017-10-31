package com.basic.spark.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

/**
 * locate com.basic.spark.streaming
 * Created by 79875 on 2017/10/31.
 * 基于窗口的操作取TOPN
 */
public class WindowBasedTopN {
    public static void main(String[] args) {
        SparkConf conf=new SparkConf().setAppName("TransformOperator")
                .setMaster("local[2]");
        JavaSparkContext sc=new JavaSparkContext(conf);
        JavaStreamingContext jsc=new JavaStreamingContext(conf, Durations.seconds(5));

        //这里叫log日志
        //tanjie hello
        //zhangfan world
        JavaReceiverInputDStream<String> lines = jsc.socketTextStream("root2", 8888);
        JavaDStream<String> logsRDD = lines.map(new Function<String, String>() {
            @Override
            public String call(String v1) throws Exception {
                return v1.split(" ")[1];
            }
        });
        JavaPairDStream<String, Integer> pariRDD = logsRDD.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<>(s, 1);
            }
        });

        JavaPairDStream<String, Integer> wordCountRDD = pariRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        JavaPairDStream<Integer, String> tempRDD = wordCountRDD.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
            @Override
            public Tuple2<Integer, String> call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                return new Tuple2<>(stringIntegerTuple2._2, stringIntegerTuple2._1);
            }
        });


        tempRDD.foreachRDD(new Function2<JavaPairRDD<Integer, String>, Time, Void>() {
            @Override
            public Void call(JavaPairRDD<Integer, String> v1, Time v2) throws Exception {
                JavaPairRDD<Integer, String> sortByKeyRDD = v1.sortByKey(false);
                sortByKeyRDD.foreach(new VoidFunction<Tuple2<Integer, String>>() {
                    @Override
                    public void call(Tuple2<Integer, String> integerStringTuple2) throws Exception {
                        System.out.println(integerStringTuple2._1+" "+integerStringTuple2._2);
                    }
                });
                return null;
            }
        });

        jsc.start();
        jsc.awaitTermination();
        jsc.close();
    }
}
