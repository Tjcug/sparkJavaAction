package com.basic.spark.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

/**
 * locate com.basic.spark.sql
 * Created by 79875 on 2017/10/30.
 * SparkSQL 加载和保存操作 保存为.parquet文件
 */
public class LoadSave {
    public static void main(String[] args) {
        SparkConf conf=new SparkConf().setAppName("DataFrameCreate")
                .setMaster("local[2]");

        JavaSparkContext sc=new JavaSparkContext(conf);
        SQLContext sqlContext=new SQLContext(sc);

        // .parquet是Spark默认的本地列式存储数据格式
        DataFrame userDF = sqlContext.read().json("data/json/students.json");
        userDF.printSchema();
        userDF.show();
        userDF.select("name").write().save("data/json/username.parquet");
    }
}
