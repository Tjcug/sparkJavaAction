package com.basic.spark.operator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * locate com.basic.spark.operator
 * Created by 79875 on 2017/10/23.
 * RDD Coalesce操作算子
 */
public class CoalesceOperator {
    public static void main(String[] args) {
        SparkConf conf=new SparkConf().setAppName("Coalescec操作算子")
                .setMaster("local[2]");
        JavaSparkContext sc=new JavaSparkContext(conf);

        //coalesce算子，功能是将parition的数量缩减，减少！！！
        //将一定的数据压缩到更少的partition分区中去
        //使用场景！很多时候在filter算子应用之后会优化一下使用coalesce算子
        //filter算子应用到RDD上面，说白了会应用到RDD里面的每个parition上去
        //数据倾斜，换句话说就是有可能有的partiton重点额数据更加紧凑！！！

        List<String> staffList= Arrays.asList("tanjie1","tanjie2","tanjie3","tanjie4","tanjie5","tanjie6","tanjie7","tanjie8","tanjie9","tanjie10","tanjie11","tanjie12");

        JavaRDD<String> staffRDD = sc.parallelize(staffList,6);

        JavaRDD<String> coalesceRDD = staffRDD.coalesce(3,true);//指定boolean进行强制进行shuffule
        JavaRDD<String> resultRDD = coalesceRDD.mapPartitionsWithIndex(new Function2<Integer, Iterator<String>, Iterator<String>>() {
            @Override
            public Iterator<String> call(Integer index, Iterator<String> v2) throws Exception {
                List<String> list = new ArrayList<>();
                while (v2.hasNext()) {
                    String name = v2.next();
                    String result="部门"+index+" "+name;
                    list.add(result);
                }
                return list.iterator();
            }
        },true);

        //collect 算子将数据传输到Dirver端
        List<String> resultList = resultRDD.collect();
        for(String staff:resultList){
            System.out.println(staff);
        }
    }
}
