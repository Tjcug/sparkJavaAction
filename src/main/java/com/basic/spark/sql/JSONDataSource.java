package com.basic.spark.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

/**
 * locate com.basic.spark.sql
 * Created by 79875 on 2017/10/30.
 * SparkSQL 读取Json数据源
 */
public class JSONDataSource {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("RDDToDataFrameReflection")
                .setMaster("local[2]");

        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);

        DataFrame jsonDataFrame = sqlContext.read().json("data/json/students.json");

        jsonDataFrame.registerTempTable("students_score");
        DataFrame studnetScoreDataFrame = sqlContext.sql("select * from students_score where score >80");

        //我们接下来把他给转换一下,因为这个时候DataFrame里面的元素还是Row！！！ 我们把它转换成String
        List<String> collect = studnetScoreDataFrame.toJavaRDD().map(new Function<Row, String>() {
            @Override
            public String call(Row v1) throws Exception {
                return v1.getString(0);
            }
        }).collect();

        List<String> studentsInfoJson=new ArrayList<>();
        studentsInfoJson.add("{\"name\":\"tanjie\",\"age\":18}");
        studentsInfoJson.add("{\"name\":\"zhangfan\",\"age\":24}");
        studentsInfoJson.add("{\"name\":\"lincangfu\",\"age\":23}");
        studentsInfoJson.add("{\"name\":\"tanzhenghua\",\"age\":41}");

        JavaRDD<String> studnetInfoRDD = sc.parallelize(studentsInfoJson);
        DataFrame studentInfoDataFrame = sqlContext.read().json(studnetInfoRDD);
        studentInfoDataFrame.registerTempTable("students_info");
        String sql="select * from students_info where name in(";
        for(int i=0;i<collect.size();i++){
            sql+="'"+collect.get(i)+"'";
            if(i<collect.size()-1){
                sql+=",";
            }
        }
        sql+=")";
        System.out.println(sql);

        DataFrame goodStudnetInfoDataFrame = sqlContext.sql(sql);
        goodStudnetInfoDataFrame.show();

        //将两份数据的DataFrame进行join转换算子操作
        JavaPairRDD<String, Tuple2<Integer, Integer>> joinRDD = goodStudnetInfoDataFrame.toJavaRDD().mapToPair(new PairFunction<Row, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Row v1) throws Exception {
                return new Tuple2<>(String.valueOf(v1.getAs("name")),Integer.valueOf(String.valueOf(v1.getAs("age"))));
            }
        }).join(studnetScoreDataFrame.toJavaRDD().mapToPair(new PairFunction<Row, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Row row) throws Exception {
                return new Tuple2<>(String.valueOf(row.getAs("name")),Integer.valueOf(String.valueOf(row.getAs("score"))));
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
        sc.close();
    }
}
