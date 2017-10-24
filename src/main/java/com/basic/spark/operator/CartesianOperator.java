package com.basic.spark.operator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

/**
 * locate com.basic.spark.operator
 * Created by 79875 on 2017/10/24.
 *  RDD Cartesian操作算子
 *  是shuffle算子
 */
public class CartesianOperator {
    public static void main(String[] args) {
        SparkConf conf=new SparkConf().setAppName("CartesianOperator")
                .setMaster("local[2]");
        conf.set("spark.default.parallelism","2");
        JavaSparkContext sc=new JavaSparkContext(conf);
        // 中文名 笛卡尔积
        // 比如说两个RDD，分别有10条数据，用了cartesian算子后
        // 两个RDD的每一个数据都会和另外一个RDD的每一条数据进行join
        // 最终形成一个笛卡尔乘积

        // 比如说，现在有5件衣服，5条裤子，分别属于两个RDD
        // 就是书，需要对每件衣服和裤子进行一次Join操作，尝试进行服装搭配
        List<String> clothes= Arrays.asList("T恤","夹克","皮大衣","衬衫","毛衣");
        List<String> trousers= Arrays.asList("西裤","内裤","铅笔裤","牛仔裤","毛仔裤");
        JavaRDD<String> clothesRDD = sc.parallelize(clothes);
        JavaRDD<String> trousersRDD = sc.parallelize(trousers);
        JavaPairRDD<String, String> cartesianRDD = clothesRDD.cartesian(trousersRDD);
        cartesianRDD.foreach(new VoidFunction<Tuple2<String, String>>() {
            @Override
            public void call(Tuple2<String, String> stringStringTuple2) throws Exception {
                System.out.println(stringStringTuple2);
            }
        });
        sc.close();
    }
}
