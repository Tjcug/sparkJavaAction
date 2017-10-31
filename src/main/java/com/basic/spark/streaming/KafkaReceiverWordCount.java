package com.basic.spark.streaming;

import kafka.serializer.StringDecoder;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * locate com.basic.spark.streaming
 * Created by 79875 on 2017/10/31.
 */
public class KafkaReceiverWordCount {
    public static void main(String[] args) {
        SparkConf conf=new SparkConf().setAppName("KafkaReceiverWordCount1")
                .setMaster("local[5]");
        JavaStreamingContext jsc=new JavaStreamingContext(conf, Durations.seconds(5));

        //kafkaConsumerParams kafkaConsumer消费者参数
        Map<String,Integer> topics=new HashMap<>();
        //key为kafka topic
        //value为Recevier读取数据线程个数
        topics.put("tweetswordtopic3",2);

        Map<String,String> kafkaParams=new HashMap<>();
        //kafka ConsumerParams kafkaConsumer消费者参数
        kafkaParams.put("auto.offset.reset","smallest");
        kafkaParams.put("zookeeper.connect","root2:2181,root4:2181,root5:2181");
        kafkaParams.put("group.id","WrodCountConsumerGroup10");
        kafkaParams.put("zookeeper.connection.timeout.ms","10000");

        //JavaPairReceiverInputDStream<String, String>
        //key为record的偏移量
        //value为record的数据
        JavaPairReceiverInputDStream<String, String> lines = KafkaUtils.createStream(jsc,String.class,String.class, StringDecoder.class,StringDecoder.class, kafkaParams, topics, StorageLevel.MEMORY_AND_DISK_SER_2());
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
