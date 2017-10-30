package com.basic.spark.sql;

import com.basic.spark.model.Student;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

/**
 * locate com.basic.spark.sql
 * Created by 79875 on 2017/10/30.
 */
public class RDDToDataFrameDynamic {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("RDDToDataFrameReflection")
                .setMaster("local[2]");

        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);

        JavaRDD<String> textFileRDD = sc.textFile("data/students.txt");
        JavaRDD<Row> rowJavaRDD = textFileRDD.map(new Function<String, Row>() {
            @Override
            public Row call(String v1) throws Exception {
                String[] split = v1.split(",");
                return RowFactory.create(Integer.valueOf(split[0]), split[1], Integer.valueOf(split[2]));
            }
        });

        //动态构造元数据，还有一种方式是通过反射的方式来构建出DataFreame，这里我们用的是通过动态构建元数据
        //有些时候我们一开始不确定有哪些列，而这些列需要从数据库比如mysql或者配置文件加载处理
        List<StructField> fieldList=new ArrayList<>();
        //true 代表是是否可以为空
        fieldList.add(DataTypes.createStructField("id",DataTypes.StringType,true));
        fieldList.add(DataTypes.createStructField("name",DataTypes.IntegerType,true));
        fieldList.add(DataTypes.createStructField("age",DataTypes.IntegerType,true));
        StructType schema=DataTypes.createStructType(fieldList);

        DataFrame dataFrame = sqlContext.createDataFrame(rowJavaRDD, schema);
        dataFrame.registerTempTable("stu");

        DataFrame sqlDataFrame = sqlContext.sql("select * from stu where age<=18");

        final JavaRDD<Row> resultRDD = sqlDataFrame.toJavaRDD();
        JavaRDD<Student> studentAgeRDD = resultRDD.map(new Function<Row, Student>() {
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
    }
}
