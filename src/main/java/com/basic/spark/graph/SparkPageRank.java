package com.basic.spark.graph;

import com.google.common.collect.Iterables;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

/**
 * locate com.basic.spark.graph
 * Created by 79875 on 2017/10/30.
 */
public class SparkPageRank {
    public static void main(String[] args) {
        SparkConf conf=new SparkConf().setAppName("SparkPageRank")
                .setMaster("local[2]");
        conf.set("spark.default.parallelism","2");
        JavaSparkContext sc=new JavaSparkContext(conf);
        sc.setCheckpointDir("hdfs://root2:9000/user/79875/sparkcheckpointJava");

        final int iters=20;
        JavaRDD<String> textFileRDD = sc.textFile("data/page.txt");
        textFileRDD.cache();

        JavaPairRDD<Integer, Integer> graphRDD = textFileRDD.mapToPair(new PairFunction<String, Integer, Integer>() {
            @Override
            public Tuple2<Integer, Integer> call(String s) throws Exception {
                String[] split = s.split(" ");
                return new Tuple2<>(Integer.valueOf(split[0]), Integer.valueOf(split[1]));
            }
        });
        JavaPairRDD<Integer, Iterable<Integer>> linksRDD = graphRDD.distinct().groupByKey().cache();

        JavaPairRDD<Integer, Double> ranksRDD = linksRDD.mapValues(new Function<Iterable<Integer>, Double>() {
            @Override
            public Double call(Iterable<Integer> v1) throws Exception {
                return 1.0;
            }
        });

        for(int i=0;i<iters;i++){
            JavaPairRDD<Integer, Double> contribsRDD = linksRDD.join(ranksRDD).values()
                    .flatMapToPair(new PairFlatMapFunction<Tuple2<Iterable<Integer>, Double>, Integer, Double>() {
                        @Override
                        public Iterable<Tuple2<Integer, Double>> call(Tuple2<Iterable<Integer>, Double> iterableDoubleTuple2) throws Exception {
                            int urlCount = Iterables.size(iterableDoubleTuple2._1);
                            List<Tuple2<Integer, Double>> results = new ArrayList<>();
                            for (Integer n : iterableDoubleTuple2._1) {
                                results.add(new Tuple2<>(n,iterableDoubleTuple2._2() / urlCount));
                            }
                            return results;
                        }
                    });

            ranksRDD= contribsRDD.reduceByKey(new Function2<Double, Double, Double>() {
                @Override
                public Double call(Double v1, Double v2) throws Exception {
                    return v1+v2;
                }
            }).mapValues(new Function<Double, Double>() {
                @Override
                public Double call(Double v1) throws Exception {
                    return 0.15+0.8*v1;
                }
            });
        }

        for (Tuple2<Integer, Double> integerDoubleTuple2 : ranksRDD.collect()) {
            System.out.println(integerDoubleTuple2._1+" "+integerDoubleTuple2._2);
        }

        sc.close();
    }
}
