package com.basic.spark.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;

/**
 * locate com.basic.spark.core
 * Created by 79875 on 2017/10/24.
 * Spark 内存缓存级别
 */
public class PersistenLevelTest {
    public static void main(String[] args) {
        SparkConf conf=new SparkConf().setAppName("SecondSort")
                .setMaster("local");
        JavaSparkContext sc=new JavaSparkContext(conf);

        JavaRDD<String> textFileRDD = sc.textFile("data/score.txt");

        textFileRDD.persist(StorageLevel.MEMORY_ONLY());
        textFileRDD.persist(StorageLevel.MEMORY_ONLY_SER());
    }
}
