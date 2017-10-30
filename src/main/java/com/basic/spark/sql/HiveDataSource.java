package com.basic.spark.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;

/**
 * locate com.basic.spark.sql
 * Created by 79875 on 2017/10/30.
 * 0.把hive里面的hive-site.xml文件放到spark/conf/目录下
 * 1.启动Hive
 * 2.您首先启动Mysql
 * 3.启动HDFS
 * 4.初始化HiveContext
 * 5.打包运行
 *
 *
 */
public class HiveDataSource {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("HiveDataSource")
                .setMaster("local[2]");

        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);

        //这里主要是SparkContext
        HiveContext hiveContext=new HiveContext(sc.sc());

        //判断是否村粗过studnet_info这张表，如果存储过则删除
        hiveContext.sql("DROP TABLE IF EXISTS studnet_info");
        //重建
        hiveContext.sql("CREATE TABLE IF NOT EXISTS student_info(name STRING ,age INT)");
        //加载数据，之类是Hive的东西
        hiveContext.sql("LAOD DATA LOCAL INPATH '/root/TJ/students_info.txt' INTO TABLE student_info");

        //一样的方式导入其他表
        hiveContext.sql("DROP TABLE IF EXISTS studnet_score");
        hiveContext.sql("CREATE TABLE IF NOT EXISTS student_score(name STRING ,score INT)");
        hiveContext.sql("LAOD DATA LOCAL INPATH '/root/TJ/students_score.txt' INTO TABLE student_score");

        //关联两张表，查询成绩大于80分的学生
        DataFrame goodstudentDF = hiveContext.sql("SELECT si.name,si.age,ss.score FROM studnet_info is JOIN student_score ss ON si.name=ss.name whree ss.score>=80");

        //我们得到这个数据存回到Hive中
        hiveContext.sql("DROP TABLE IF EXISTS good_studnet_info");
        goodstudentDF.saveAsTable("good_studnet_info");

        //然后我们如何把一个HIVE表进行读取过来转换成DataFrame
        DataFrame good_studnet_info = hiveContext.table("good_studnet_info");

        for (Row row : good_studnet_info.collect()) {
            System.out.println(row);
        }
    }
}
