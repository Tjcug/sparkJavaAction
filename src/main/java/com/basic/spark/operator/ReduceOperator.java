package com.basic.spark.operator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

import java.util.Arrays;
import java.util.List;

/**
 * locate com.basic.spark.operator
 * Created by 79875 on 2017/10/24.
 * RDD Reduce操作算子
 * shuffle算子
 */
public class ReduceOperator {
    public static void main(String[] args) {
        SparkConf conf=new SparkConf().setAppName("ReduceOperator")
                .setMaster("local[2]");
        conf.set("spark.default.parallelism","2");//项目中一般设置默认并行度 shuffle操作算子后生成的RDD默认分区个数
        JavaSparkContext sc=new JavaSparkContext(conf);

        List<Integer> numbersList= Arrays.asList(1,2,3,4,5);
        JavaRDD<Integer> numbersRDD = sc.parallelize(numbersList);

        //recue操作的原理：首先将第一个元素和第二个元素，传入Call方法
        //计算第一个结果，接着把结果再和后面的元素进行累加
        //以此类推
        Integer total = numbersRDD.reduce(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });
        sc.close();
        System.out.println(total);
    }
}
