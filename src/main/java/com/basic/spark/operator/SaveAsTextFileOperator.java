package com.basic.spark.operator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import java.util.Arrays;
import java.util.List;

/**
 * locate com.basic.spark.operator
 * Created by 79875 on 2017/10/24.
 * RDD SaveAsTextFile操作算子
 * 可以存储到本地文件中 可以存储到HDFDS中
 * Action操作
 */
public class SaveAsTextFileOperator {
    public static void main(String[] args) {
        SparkConf conf=new SparkConf().setAppName("MapOperator")
                .setMaster("local");
        conf.set("spark.default.parallelism","2");
        JavaSparkContext sc=new JavaSparkContext(conf);

        List<Integer> numbers= Arrays.asList(1,2,3,4,5);
        JavaRDD<Integer> numbersRDD=sc.parallelize(numbers);

        //map对每个元素进行操作
        JavaRDD<Integer> mapRDD = numbersRDD.map(new Function<Integer, Integer>() {
            @Override
            public Integer call(Integer v1) throws Exception {
                return v1 * 10;
            }
        });

        mapRDD.saveAsTextFile("hdfs://root2:9000/user/79875/saveAsTextFile");
        sc.close();
    }
}
