package com.basic.spark.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;

/**
 * locate com.basic.spark.streaming
 * Created by 79875 on 2017/10/31.
 * SparkStreaming WordCount程序
 * yum install nc
 * nc -lk 8888
 */
public class StreamingWordCount {
    public static void main(String[] args) {
        /**
         * 创建该对象，类似与Spark Core中的JavaSparkContext，类似于SparkSQL中SQLContext
         * 该对象除了接受SparkConf对象，还接受一个BtachInterval参数，就是说，每收集多长时间的数据划分为一个Batch即RDD去执行
         * 这里Durations可以指定分钟，毫秒，秒
         */
        SparkConf conf=new SparkConf().setAppName("StreamingWordCount")
                .setMaster("local[2]");
        JavaStreamingContext jsc=new JavaStreamingContext(conf, Durations.seconds(1));

        /**
         * 首先创建一个DStream，代表一个数据源比如质量从socket或者Kafka中持续不断你进入实时数据量
         * 创建一个监听socket数据量，RDD里面的每一个元素都是一行行的文本
         */
        JavaReceiverInputDStream<String> lines = jsc.socketTextStream("root2", 8888);
        JavaDStream<String> wordsRDD = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterable<String> call(String s) throws Exception {
                return Arrays.asList(s.split(" "));
            }
        });

        JavaPairDStream<String, Integer> wordPairRDD = wordsRDD.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<>(s, 1);
            }
        });

        JavaPairDStream<String, Integer> wrodCountRDD = wordPairRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        //最后每一次计算完成，都打印wordcount
        wrodCountRDD.print();

        jsc.start();
        jsc.awaitTermination();
        jsc.close();
    }
}
