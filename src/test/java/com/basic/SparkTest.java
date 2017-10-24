package com.basic;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;

import java.util.Arrays;
import java.util.List;

/**
 * locate com.basic
 * Created by 79875 on 2017/10/24.
 */
public class SparkTest {
    private static SparkConf conf = new SparkConf().setAppName("RepartitionOperator")
            .setMaster("local[5]");

    public static void main(String[] args) {
        conf.set("spark.default.parallelism","2");//项目中一般设置默认并行度 shuffle操作算子后生成的RDD默认分区个数
        JavaSparkContext sc=new JavaSparkContext(conf);
        List<String> staffList= Arrays.asList("tanjie1","tanjie2","tanjie3","tanjie4","tanjie5","tanjie6","tanjie7","tanjie8","tanjie9","tanjie10","tanjie11","tanjie12");
        JavaRDD<String> staffRDD = sc.parallelize(staffList);
        JavaRDD<String> repartitionRDD = staffRDD.repartition(10);

        JavaRDD<String> mapRDD = repartitionRDD.map(new Function<String, String>() {
            @Override
            public String call(String v1) throws Exception {
                return v1 + " 1";
            }
        });

        mapRDD.foreach(new VoidFunction<String>() {
            @Override
            public void call(String s) throws Exception {
                System.out.println(s);
            }
        });
        System.out.println(mapRDD.splits().size());
        sc.close();
    }
}
