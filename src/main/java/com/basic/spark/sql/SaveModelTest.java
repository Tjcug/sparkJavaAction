package com.basic.spark.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;

/**
 * locate com.basic.spark.sql
 * Created by 79875 on 2017/10/30.
 * SparkSQL Save模型
 */
public class SaveModelTest {
    public static void main(String[] args) {
        SparkConf conf=new SparkConf().setAppName("SaveModelTest")
                .setMaster("local");

        JavaSparkContext sc=new JavaSparkContext(conf);
        SQLContext sqlContext=new SQLContext(sc);

        // .parquet是Spark默认的本地列式存储数据格式
        DataFrame userDF = sqlContext.read().format("json").load("data/json/students.json");

        userDF.save("data/json/students2.json", SaveMode.ErrorIfExists);//如果目标文件存在的话就报错
        userDF.save("data/json/students2.json", SaveMode.Append);//如果目标文件存在的话就追加操作
        userDF.save("data/json/students2.json", SaveMode.Ignore);//如果目标文件存在的话就忽略
        userDF.save("data/json/students2.json", SaveMode.Overwrite);//如果目标文件存在的话就覆盖
    }
}
