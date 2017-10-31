package com.basic.spark.streaming;

import kafka.serializer.StringDecoder;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

import java.util.*;

/**
 * locate com.basic.spark.streaming
 * Created by 79875 on 2017/10/31.
 * 特性：
 * 1.Simplified Parallelism:没有必要创建多个Kafka输入流通过DirectStream的方式，SparkStreaming将会创建多个RDD分区它们对应相应的Kakfa分区消费
 * 2.Efficiency:为了取实现0数据丢失在第一次approach，需要我们把数据WAL(Write Ahead Log)写入到日志中，这样就复制了多份数据，这样是没有效率的，这样数据被保存了两份
 * 但是这种Direct方式将没有这种问题，因为它没有Receiver。并且没有必要将数据WAL(Write Ahead Log)写入到日志中。只要我们有足够的Kafka保存时间就可以了。
 * 3.Exactly-once- semantics:这种Receiver方式使用了Kafka高等级API将consumer的Offest保存到zookeeper中。这种方式是传统的Kafka消费方式。当然Receiver这种
 * 方式也能确保0数据丢失（in combinaion with write ahead logs）At Least Once.第二种Direct方式Offests被跟踪方法到SparkStreaming checkpoints中。exactly-once
 * 这种方式SparkStreaming自己存着 Offest由SparkStreaming自己管理，SparkStreaming可以自己知道哪些数据读取了哪些数据没有读取
 */
public class KafkaDirectWordCount {
    public static void main(String[] args) {
        SparkConf conf=new SparkConf().setAppName("KafkaDirectWordCount")
                .setMaster("local[2]");
        JavaStreamingContext jsc=new JavaStreamingContext(conf, Durations.seconds(1));
        Map<String,String> kafkaParams=new HashMap<>();
        //key为kafka topic
        //value为Recevier读取数据线程个数
        kafkaParams.put("metadata.broker.list","root8:9092,root9:9092,root10:9092");
        kafkaParams.put("auto.offset.reset","smallest");

        Set<String> topics=new HashSet<>();
        //key为kafka topic
        //value为Recevier读取数据线程个数
        topics.add("tweetswordtopic3");
        JavaPairInputDStream<String, String> lines = KafkaUtils.createDirectStream(
                jsc,
                String.class,
                String.class,
                StringDecoder.class,
                StringDecoder.class,
                kafkaParams,
                topics
        );

        JavaDStream<String> wordsRDD = lines.flatMap(new FlatMapFunction<Tuple2<String, String>, String>() {
            @Override
            public Iterable<String> call(Tuple2<String, String> stringStringTuple2) throws Exception {
                return Arrays.asList(stringStringTuple2._2.split(" "));
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
