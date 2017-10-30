package com.basic.spark.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

/**
 * locate com.basic.spark.sql
 * Created by 79875 on 2017/10/30.
 * SparkSQL 创建一个DataFreame数据框
 */
public class DataFrameCreate {
    public static void main(String[] args) {
        SparkConf conf=new SparkConf().setAppName("DataFrameCreate")
                .setMaster("local");

        JavaSparkContext sc=new JavaSparkContext(conf);
        SQLContext sqlContext=new SQLContext(sc);

        DataFrame jsonDataFrame = sqlContext.read().json("data/json/students.json");
        jsonDataFrame.show();

        sc.close();
    }
}
