package com.basic.spark.operator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * locate com.basic.spark.operator
 * Created by 79875 on 2017/10/24.
 *  RDD CountByKey操作算子
 *  CountByKey=GrouByKey + Count
 *  不是shuffle算子,是一个Action操作 重要的事情说三遍
 */
public class CountByKeyOperator {
    public static void main(String[] args) {
        SparkConf conf=new SparkConf().setAppName("CountByKeyOperator")
                .setMaster("local[2]");
        conf.set("spark.default.parallelism","2");
        JavaSparkContext sc=new JavaSparkContext(conf);

        //准备一下数据
        List<Tuple2<Integer,String>> scoreList= Arrays.asList(
                new Tuple2<Integer,String>(100,"tanjie"),
                new Tuple2<Integer,String>(60,"tanjie"),
                new Tuple2<Integer,String>(50,"zhangfan"),
                new Tuple2<Integer,String>(20,"lincangfu"),
                new Tuple2<Integer,String>(60,"zhangfan")
        );
        //对RDD进行countByKey算子，统计每个70或者80 人数分别是多少
        //说白了就是统计每种Key对应的元素个数
        JavaPairRDD<Integer, String> javaPairRDD = sc.parallelizePairs(scoreList);
        Map<Integer, Object> countByKey = javaPairRDD.countByKey();
        for(Integer integer:countByKey.keySet()){
            System.out.println(integer+" "+countByKey.get(integer));
        }

        sc.close();
    }
}
