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
 *  RDD SortByKey操作算子
 * 根据Key进行排序操作
 * 是shuffle算子
 */
public class SortByKeyOperator {
    public static void main(String[] args) {
        SparkConf conf=new SparkConf().setAppName("SortByKeyOperator")
                .setMaster("local[2]");
        conf.set("spark.default.parallelism","2");//项目中一般设置默认并行度 shuffle操作算子后生成的RDD默认分区个数
        JavaSparkContext sc=new JavaSparkContext(conf);

        List<Tuple2<Integer,String>> scoreList= Arrays.asList(
                new Tuple2<Integer,String>(100,"tanjie"),
                new Tuple2<Integer,String>(60,"tanjie"),
                new Tuple2<Integer,String>(50,"zhangfan"),
                new Tuple2<Integer,String>(20,"lincangfu"),
                new Tuple2<Integer,String>(60,"zhangfan")
        );

        JavaPairRDD<Integer, String> javaPairRDD = sc.parallelizePairs(scoreList);
        //false为降序 true为升序
        JavaPairRDD<Integer, String> sortByKeyRDD = javaPairRDD.sortByKey(true);

        sortByKeyRDD.foreach(new VoidFunction<Tuple2<Integer, String>>() {
            @Override
            public void call(Tuple2<Integer, String> integerStringTuple2) throws Exception {
                System.out.println(integerStringTuple2._2());
            }
        });

        sc.close();
    }
}
