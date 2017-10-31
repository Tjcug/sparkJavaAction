package com.basic.spark.streaming;

import com.google.common.base.Optional;
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
import java.util.List;

/**
 * locate com.basic.spark.streaming
 * Created by 79875 on 2017/10/31.
 * UpdateStateByKey SparkStreaming独特算子Operator
 * Stateful 有状态的实时流处理
 */
public class UpdateStateByWordCount {
    public static void main(String[] args) {
        SparkConf conf=new SparkConf().setAppName("StreamingWordCount")
                .setMaster("local[2]");
        JavaStreamingContext jsc=new JavaStreamingContext(conf, Durations.seconds(1));
        //对于状态Stateful进行checkpoints
        jsc.checkpoint("hdfs://root2:9000/user/79875/sparkcheckpoint");

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

        JavaPairDStream<String, Integer> wrodCountRDD = wordPairRDD.updateStateByKey(new Function2<List<Integer>, Optional<Integer>, Optional<Integer>>() {
            //首先第一步对相同的key进行分组
            //实际上，对于每个单词，每次batch计算的时候，都会调用这个函数买第一个参数values相当于这个batch中
            //这个key对应新的一组值，可能有多个，可能有两个1(tanjie,1)(tanjie,1),纳米这个values就是（1，1)
            //纳米第二个参数表示的是这个key之前的状态，我们看类型Integer你也就知道了，这里是泛型咱们自己指定的

            @Override
            public Optional<Integer> call(List<Integer> values, Optional<Integer> state) throws Exception {
                //Optional其实有两个子类，一个子类是Some，另一个子类是None
                //就是key有可能之前从来没有出现过，意味着之前从来没有更新过状态
                Integer newVaule=0;
                if(state.isPresent()){
                    newVaule=state.get();
                }
                for(Integer value:values){
                    newVaule+=value;
                }
                return Optional.of(newVaule);
            }
        });

        //最后每一次计算完成，都打印wordcount
        wrodCountRDD.print();

        jsc.start();
        jsc.awaitTermination();
        jsc.close();
    }
}
