package com.basic.spark.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;

/**
 * locate com.basic.spark.core
 * Created by 79875 on 2017/10/30.
 * 开窗函数
 * 说白了就是SparkSQL做咱们的分组取TOPN
 *
 * spark-submit --master spark://root2:7077 --class com.basic.spark.core.RowNumberWindowFunction sparkJavaAction-1.0-SNAPSHOT.jar
 */
public class RowNumberWindowFunction {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("RowNumberWindowFunction")
                .setMaster("local[2]");

        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);
        //这里主要是SparkContext
        HiveContext hiveContext=new HiveContext(sc.sc());

        //创建销售额表，创建sales表
//        hiveContext.sql("DROP TABLE IF EXISTS sales");
//        hiveContext.sql("CREATE TABLE IF NOT EXISTS sales(product STRING ,category STRING,revenue BIGINT)");
//        hiveContext.sql("LAOD DATA LOCAL INPATH '/root/TJ/sales.txt' INTO TABLE sales");

        //首先说明一下开创函数的作用
        //row_number()开窗函数

        DataFrame top3SalesDF = hiveContext.sql("SELECT product,category,revenue FROM(SELECT product,category,revenue,row_number() OVER(PARTITION BY category ORDER BY revenue DESC) rank FROM sales) tmp_sales" +
                " where rank<=3");

        //将每组排名前三的数据，保存到一个表中
        hiveContext.sql("DROP TABLE IF EXISTS top3_sales");
        top3SalesDF.saveAsTable("top3_sales");
        sc.close();
    }
}
