package com.basic.spark.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

/**
 * locate com.basic.spark.sql
 * Created by 79875 on 2017/10/30.
 * SparkSQL DataFrame操作
 */
public class DataFrameOperator {
    public static void main(String[] args) {
        SparkConf conf=new SparkConf().setAppName("DataFrameCreate")
                .setMaster("local[2]");

        JavaSparkContext sc=new JavaSparkContext(conf);
        SQLContext sqlContext=new SQLContext(sc);

        //把数据框读取过来完全可以理解为一张表
        DataFrame jsonDataFrame = sqlContext.read().json("data/json/students.json");

        //打印这张表
        jsonDataFrame.show();

        //打印元数据
        jsonDataFrame.printSchema();

        //查询并列计算
        jsonDataFrame.select("name").show();
        jsonDataFrame.select(jsonDataFrame.col("name"),jsonDataFrame.col("score").plus(1)).show();//对socre列值进行加一

        //过滤
        jsonDataFrame.filter(jsonDataFrame.col("score").gt(80)).show();

        //根据某一列进行分组然后统计
        jsonDataFrame.groupBy("score").count().show();

        sc.close();
    }
}
