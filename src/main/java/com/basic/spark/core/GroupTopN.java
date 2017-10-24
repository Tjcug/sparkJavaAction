package com.basic.spark.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.*;

/**
 * locate com.basic.spark.core
 * Created by 79875 on 2017/10/24.
 * 分组取TOPN问题
 */
public class GroupTopN {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("TOPN")
                .setMaster("local");
        final JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> textFileRDD = sc.textFile("data/score.txt");
        JavaPairRDD<String, Integer> pairRDD = textFileRDD.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                String[] split = s.split(" ");
                return new Tuple2<>(split[0], Integer.valueOf(split[1]));
            }
        });

        JavaPairRDD<String, Iterable<Integer>> groupByKeyRDD = pairRDD.groupByKey();

        JavaRDD<Tuple2<String, Iterable<Integer>>> top2ScoreRDD = groupByKeyRDD.map(new Function<Tuple2<String, Iterable<Integer>>, Tuple2<String, Iterable<Integer>>>() {
            @Override
            public Tuple2<String, Iterable<Integer>> call(Tuple2<String, Iterable<Integer>> v1) throws Exception {
                Iterator<Integer> iterator = v1._2().iterator();
                List<Integer> scores = new ArrayList<Integer>();
                while (iterator.hasNext()) {
                    scores.add(iterator.next());
                }
                Collections.sort(scores, new Comparator<Integer>() {
                    @Override
                    public int compare(Integer o1, Integer o2) {
                        return -(o1 - o2);
                    }
                });
                scores = scores.subList(0, 2);
                return new Tuple2<String, Iterable<Integer>>(v1._1, scores);
            }
        });

        top2ScoreRDD.foreach(new VoidFunction<Tuple2<String, Iterable<Integer>>>() {
            @Override
            public void call(Tuple2<String, Iterable<Integer>> stringIterableTuple2) throws Exception {
                System.out.println(stringIterableTuple2._1+" "+stringIterableTuple2._2);
            }
        });
        sc.close();
    }
}
