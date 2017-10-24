package com.basic.spark.operator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

/**
 * locate com.basic.spark.operator
 * Created by 79875 on 2017/10/24.
 * RDD RedcueByKey操作算子
 * 是shuffle算子
 *
 * reduceByKey=groupByKey+reduce
 * shuffle洗牌=map端+reduce端
 * spark里面这个reduceByKey再map端里面自带Combiner
 */
public class RedcueByKeyOperator {
    public static void main(String[] args) {
        SparkConf conf=new SparkConf().setAppName("RedcueByKeyOperator")
                .setMaster("local[2]");
        conf.set("spark.default.parallelism","2");
        JavaSparkContext sc=new JavaSparkContext(conf);

        List<Tuple2<String,Integer>> scoreList= Arrays.asList(
                new Tuple2<String,Integer>("tanjie",100),
                new Tuple2<String,Integer>("tanjie",60),
                new Tuple2<String,Integer>("zhangfan",50),
                new Tuple2<String,Integer>("lincangfu",20),
                new Tuple2<String,Integer>("zhangfan",60)
        );

        JavaPairRDD<String, Integer> scoreRDD = sc.parallelizePairs(scoreList);
        JavaPairRDD<String, Integer> redcueRDD = scoreRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        redcueRDD.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            @Override
            public void call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                System.out.println(stringIntegerTuple2._1+" "+stringIntegerTuple2._2());
            }
        });

        sc.close();
    }
}
