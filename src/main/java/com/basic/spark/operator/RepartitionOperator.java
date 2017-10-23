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
 * RDD Repartition操作算子
 */
public class RepartitionOperator {
    public static void main(String[] args) {
        SparkConf conf=new SparkConf().setAppName("RepartitionOperator")
                .setMaster("local[2]");
        JavaSparkContext sc=new JavaSparkContext(conf);

        //repartition算子，用于任意将RDD的partition增多或者减少
        // coalesce仅仅将RDD的partiton减少
        //建议使用的场景
        //一个很经典的场景，使用 SparkSQL从HIVE查询数据时候，SparkSQL会根据HIVE对应的HDFS
        // 文件的block的数量决定加载处理的RDD的partition有多少个
        //这里默认的partition的数量我们根本是无法设置的

        //有时候，可能会自动设置的partition的数量过于少了，为了进行优化
        //可以提高并行度，就是对RDD使用repartiton算子

        List<String> staffList= Arrays.asList("tanjie1","tanjie2","tanjie3","tanjie4","tanjie5","tanjie6","tanjie7","tanjie8","tanjie9","tanjie10","tanjie11","tanjie12");

        JavaRDD<String> staffRDD = sc.parallelize(staffList,3);

        JavaRDD<String> resultRDD = staffRDD.mapPartitionsWithIndex(new Function2<Integer, Iterator<String>, Iterator<String>>() {
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

        List<String> resultList = resultRDD.collect();
        for(String staff:resultList){
            System.out.println(staff);
        }

        System.out.println("--------------------------repartition---------------------------");
        JavaRDD<String> repartition = staffRDD.repartition(6);
        JavaRDD<String> repartitionRDD = repartition.mapPartitionsWithIndex(new Function2<Integer, Iterator<String>, Iterator<String>>() {
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
        resultList = repartitionRDD.collect();
        for(String staff:resultList){
            System.out.println(staff);
        }
    }
}
