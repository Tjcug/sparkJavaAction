package com.basic.spark.streaming;

import com.google.common.base.Optional;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

/**
 * locate com.basic.spark.streaming
 * Created by 79875 on 2017/10/31.
 * SparkStreaming TransformOperator算子操作
 * 对于DStreamRDD与RDD进行操作
 */
public class TransformOperator {
    public static void main(String[] args) {
        SparkConf conf=new SparkConf().setAppName("TransformOperator")
                .setMaster("local[2]");
        JavaSparkContext sc=new JavaSparkContext(conf);
        JavaStreamingContext jsc=new JavaStreamingContext(conf, Durations.seconds(1));

        /**
         * 用户对于网站上的广告进行点击！点击之后进行实时计算，但是有些用户就是刷广告
         * 所以我们有一个黑名单机制！只要是黑名单上的用户发的广告我们就过滤掉
         */
        List<Tuple2<String,Boolean>> blackList=new ArrayList<>();
        blackList.add(new Tuple2<String, Boolean>("tanjie",true));
        blackList.add(new Tuple2<String, Boolean>("zhangfan",false));
        final JavaPairRDD<String, Boolean> blackRDD = sc.parallelizePairs(blackList);

        //time adID name
        JavaReceiverInputDStream<String> adsClickLogDStream = jsc.socketTextStream("root2", 8888);
        JavaPairDStream<String, String> adsClickLogPairDStream = adsClickLogDStream.mapToPair(new PairFunction<String, String, String>() {
            @Override
            public Tuple2<String, String> call(String s) throws Exception {
                return new Tuple2<>(s.split(" ")[2], s);
            }
        });

        JavaDStream<String> transformRDD = adsClickLogPairDStream.transform(new Function<JavaPairRDD<String, String>, JavaRDD<String>>() {
            @Override
            public JavaRDD<String> call(JavaPairRDD<String, String> userBatchLogRDD) throws Exception {
                JavaPairRDD<String, Tuple2<String, Optional<Boolean>>> joinRDD = userBatchLogRDD.leftOuterJoin(blackRDD);
                JavaPairRDD<String, Tuple2<String, Optional<Boolean>>> filterRDD = joinRDD.filter(new Function<Tuple2<String, Tuple2<String, Optional<Boolean>>>, Boolean>() {
                    @Override
                    public Boolean call(Tuple2<String, Tuple2<String, Optional<Boolean>>> v1) throws Exception {
                        if (v1._2._2.isPresent() && v1._2._2.get()) {
                            return false;
                        }
                        return true;
                    }
                });

                JavaRDD<String> resultRDD = filterRDD.map(new Function<Tuple2<String, Tuple2<String, Optional<Boolean>>>, String>() {
                    @Override
                    public String call(Tuple2<String, Tuple2<String, Optional<Boolean>>> v1) throws Exception {
                        return v1._2._1;
                    }
                });
                return resultRDD;
            }
        });

        transformRDD.print();

        jsc.start();
        jsc.awaitTermination();
        jsc.close();
    }
}
