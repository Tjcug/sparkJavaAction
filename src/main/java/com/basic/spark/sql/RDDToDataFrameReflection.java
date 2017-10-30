package com.basic.spark.sql;

import com.basic.spark.model.Student;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

/**
 * locate com.basic.spark.sql
 * Created by 79875 on 2017/10/30.
 * RDD 转换为DataFrame 通过反射机制
 */
public class RDDToDataFrameReflection {
    public static void main(String[] args) {
        SparkConf conf=new SparkConf().setAppName("RDDToDataFrameReflection")
                .setMaster("local[2]");

        JavaSparkContext sc=new JavaSparkContext(conf);
        SQLContext sqlContext=new SQLContext(sc);

        JavaRDD<String> textFileRDD = sc.textFile("data/students.txt");
        JavaRDD<Student> studnetRDD = textFileRDD.map(new Function<String, Student>() {
            @Override
            public Student call(String v1) throws Exception {
                String[] split = v1.split(",");
                return new Student(Integer.valueOf(split[0]), split[1], Integer.valueOf(split[2]));
            }
        });

        //通过反射方式创建DataFrame
        DataFrame studnetDF = sqlContext.createDataFrame(studnetRDD, Student.class);
        studnetDF.printSchema();
        //有了DataFrame后就可以注册一个临时表，SQL语句还是查询年龄小于18的人
        studnetDF.registerTempTable("student");

        DataFrame sqlDataFrame = sqlContext.sql("select * from student where age<=18");
        final JavaRDD<Row> rowJavaRDD = sqlDataFrame.toJavaRDD();
        JavaRDD<Student> studentAgeRDD = rowJavaRDD.map(new Function<Row, Student>() {
            @Override
            public Student call(Row v1) throws Exception {
                //通过反射来生成这个DataFrame的方式使用get(index),大家注意这个列的顺序是字典顺序
                //int id=row.getInt(1)
                //String name=row.getString(2)
                //int age=row.getInt(0)

                //第二种可以通过列名字来获取Row里面的数据，这样的好处就是不用担心上面的顺序了
                int id = v1.getAs("id");
                String name = v1.getAs("name");
                int age = v1.getAs("age");
                return new Student(id, name, age);
            }
        });

        studentAgeRDD.foreach(new VoidFunction<Student>() {
            @Override
            public void call(Student student) throws Exception {
                System.out.println(student);
            }
        });

        sc.close();
    }
}
