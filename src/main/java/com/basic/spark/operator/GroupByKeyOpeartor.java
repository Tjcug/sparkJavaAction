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
 * GroupByKeyOpeartor 算子操作
 * shuffle算子
 */
public class GroupByKeyOpeartor {
    public static void main(String[] args) {
        SparkConf conf=new SparkConf().setAppName("GroupByKeyOpeartor")
                .setMaster("local[2]");
        conf.set("spark.default.parallelism","2");//项目中一般设置默认并行度 shuffle操作算子后生成的RDD默认分区个数
        JavaSparkContext sc=new JavaSparkContext(conf);

        List<Tuple2<String,Integer>> scoreList= Arrays.asList(
                new Tuple2<String,Integer>("tanjie",100),
                new Tuple2<String,Integer>("tanjie",60),
                new Tuple2<String,Integer>("zhangfan",50),
                new Tuple2<String,Integer>("lincangfu",20),
                new Tuple2<String,Integer>("zhangfan",60)
        );

        JavaPairRDD<String, Integer> scoreRDD = sc.parallelizePairs(scoreList);
        //groupByKey把相同的key的元素放到一起去
        //scoreRDD.groupByKey(numpartition) groupByKey算子操作后RDD的分区个数

        scoreRDD.groupByKey().foreach(new VoidFunction<Tuple2<String, Iterable<Integer>>>() {
            @Override
            public void call(Tuple2<String, Iterable<Integer>> stringIterableTuple2) throws Exception {
                System.out.println(stringIterableTuple2._1+" "+stringIterableTuple2._2);
            }
        });

        sc.close();
    }
}
