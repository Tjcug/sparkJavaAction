package com.basic.spark.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * locate com.basic.spark.sql
 * Created by 79875 on 2017/10/30.
 *
 * ./bin/spark-submit --master spark://root2:7077 --class com.basic.spark.sql.JDBCDataSource
 *                  --driver-class-path ./lib/mysql-connector-java-5.1.32-bin.jar
 *                  --jars ./lib/mysql-connector-java-5.1.32-bin.jar
 *                  sparkJavaAction-1.0-SNAPSHOT.jar
 *
 *  如果要运行在yarn模式下s
 * 在conf/spark-defaults.conf里面配置下面两行
 * spark.driver.extraClassPath=/home/tj/softwares/spark-1.6.0-bin-hadoop2.6/lib/mysql-connector-5.1.8-bin.jar
 * spark.executor.extraClassPath=/home/tj/softwares/spark-1.6.0-bin-hadoop2.6/lib/mysql-connector-5.1.8-bin.jar
 */
public class JDBCDataSource {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("JDBCDataSource")
                .setMaster("local[2]");

        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);

        Map<String, String> option = new HashMap<>();
        option.put("url","jdbc:mysql://localhost:3306/sparksql");
        option.put("dbtable","studentinfo");
        option.put("user","root");
        option.put("password","123456");

        DataFrame studentInfoDataFrame = sqlContext.read().format("jdbc").options(option).load();

        option.put("dbtable","studentscore");
        DataFrame studentScoreDataFrame = sqlContext.read().format("jdbc").options(option).load();

        //我们将两个DataFrame转换成JavaPairRDD 进行join操作
        JavaPairRDD<String, Tuple2<Integer, Integer>> joinRDD = studentInfoDataFrame.toJavaRDD().mapToPair(new PairFunction<Row, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Row v1) throws Exception {
                return new Tuple2<>(String.valueOf(v1.getAs("name")), Integer.valueOf(String.valueOf(v1.getAs("age"))));
            }
        }).join(studentScoreDataFrame.toJavaRDD().mapToPair(new PairFunction<Row, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Row row) throws Exception {
                return new Tuple2<>(String.valueOf(row.getAs("name")), Integer.valueOf(String.valueOf(row.getAs("score"))));
            }
        }));

        JavaRDD<Row> goodstudentRowRDD = joinRDD.map(new Function<Tuple2<String, Tuple2<Integer, Integer>>, Row>() {
            @Override
            public Row call(Tuple2<String, Tuple2<Integer, Integer>> v1) throws Exception {
                return RowFactory.create(v1._1, v1._2._1, v1._2._2);
            }
        });

        List<StructField> fieldList=new ArrayList<>();
        //true 代表是是否可以为空
        fieldList.add(DataTypes.createStructField("name",DataTypes.StringType,true));
        fieldList.add(DataTypes.createStructField("age",DataTypes.IntegerType,true));
        fieldList.add(DataTypes.createStructField("score",DataTypes.IntegerType,true));
        StructType schema=DataTypes.createStructType(fieldList);

        DataFrame dataFrame = sqlContext.createDataFrame(goodstudentRowRDD, schema);
        dataFrame.show();
        dataFrame.write().format("json").mode(SaveMode.Overwrite).save("hdfs://root2:9000/user/79875/output/jsonDataSource");

        dataFrame.toJavaRDD().foreach(new VoidFunction<Row>() {
            @Override
            public void call(Row row) throws Exception {
                Connection conn=JdbcPool.getConnection();
                PreparedStatement preparedStatement;
                String sql = "INSERT INTO goodstudent(name,age,score)"
                        + " VALUES (?,?,?)";  // 插入数据的sql语句
                preparedStatement = conn.prepareStatement(sql);    // 创建用于执行静态sql语句的Statement对象
                preparedStatement.setString(1,String.valueOf(row.getAs("name")));
                preparedStatement.setInt(2,Integer.valueOf(String.valueOf(row.getAs("age"))));
                preparedStatement.setInt(3,Integer.valueOf(String.valueOf(row.getAs("score"))));
                int count = preparedStatement.executeUpdate();  // 执行插入操作的sql语句，并返回插入数据的个数
                preparedStatement.close();
            }
        });
        sc.close();
    }
}
