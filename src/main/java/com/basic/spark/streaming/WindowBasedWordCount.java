package com.basic.spark.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

/**
 * locate com.basic.spark.streaming
 * Created by 79875 on 2017/10/31.
 */
public class WindowBasedWordCount {
    public static void main(String[] args) {
        SparkConf conf=new SparkConf().setAppName("TransformOperator")
                .setMaster("local[2]");
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

        //reduceByKeyAndWindow 两种实现方式
        JavaPairDStream<String, Integer> wordCountRDD = pariRDD.reduceByKeyAndWindow(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1+v2;
            }
        }, Durations.seconds(60), Durations.seconds(10));

        //如使用这种reduceByKeyAndWindow的API必须进行checkpoints
        //对中间结果进行checkpoints
        //jsc.checkpoint("hdfs://root2:9000/user/79875/sparkcheckpoint");
//        JavaPairDStream<String, Integer> wordCountRDD = pariRDD.reduceByKeyAndWindow(new Function2<Integer, Integer, Integer>() {
//            @Override
//            public Integer call(Integer v1, Integer v2) throws Exception {
//                return v1+v2;
//            }
//        }, new Function2<Integer, Integer, Integer>() {
//            @Override
//            public Integer call(Integer v1, Integer v2) throws Exception {
//                return v1-v2;
//            }
//        }, Durations.seconds(60), Durations.seconds(10));

        wordCountRDD.print();

        jsc.start();
        jsc.awaitTermination();
        jsc.close();
    }
}
