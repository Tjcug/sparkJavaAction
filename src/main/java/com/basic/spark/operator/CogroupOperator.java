package com.basic.spark.operator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

/**
 * locate com.basic.spark.operator
 * Created by 79875 on 2017/10/24.
 * RDD  Cogroup操作算子
 * 是shuffle算子
 */
public class CogroupOperator {
    public static void main(String[] args) {
        SparkConf conf=new SparkConf().setAppName("CogroupOperator")
                .setMaster("local[2]");
        conf.set("spark.default.parallelism","2");
        JavaSparkContext sc=new JavaSparkContext(conf);

        //准备一下数据
        List<Tuple2<Integer,String>> nameList= Arrays.asList(
                new Tuple2<Integer,String>(1,"tanjie"),
                new Tuple2<Integer,String>(2,"zhangfan"),
                new Tuple2<Integer,String>(2,"tanzhenghua"),
                new Tuple2<Integer,String>(3,"lincangfu")
        );

        List<Tuple2<Integer,Integer>> scoreList=Arrays.asList(
                new Tuple2<Integer, Integer>(1,100),
                new Tuple2<Integer, Integer>(2,60),
                new Tuple2<Integer, Integer>(3,90),
                new Tuple2<Integer, Integer>(1,70),
                new Tuple2<Integer, Integer>(2,50),
                new Tuple2<Integer, Integer>(3,40)

        );

        JavaPairRDD<Integer, String> nameRDD = sc.parallelizePairs(nameList);
        JavaPairRDD<Integer, Integer> scoreRDD = sc.parallelizePairs(scoreList);

        //cogroup 与 join不同
        // 相当于，一个key join上所有value，都放到一个Interable里面去
        JavaPairRDD<Integer, Tuple2<Iterable<String>, Iterable<Integer>>> cogroup = nameRDD.cogroup(scoreRDD);
        cogroup.foreach(new VoidFunction<Tuple2<Integer, Tuple2<Iterable<String>, Iterable<Integer>>>>() {
            @Override
            public void call(Tuple2<Integer, Tuple2<Iterable<String>, Iterable<Integer>>> integerTuple2Tuple2) throws Exception {
                System.out.println("student id: "+integerTuple2Tuple2._1);
                System.out.println("student name: "+integerTuple2Tuple2._2._1);
                System.out.println("student score: "+integerTuple2Tuple2._2._2);
            }
        });

        sc.close();
    }
}
