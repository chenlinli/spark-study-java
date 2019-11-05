package cn.spark.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

import java.util.Arrays;
import java.util.List;

/**
 * 并行化集合创建RDD
 */
public class ParallelizeConnection {
    public static void main(String[] args) {
        //创建sparkconf
        SparkConf sparkConf = new SparkConf().setAppName("ParallelizeConnection").setMaster("local");

        //创建sc
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);

        //并行化机会创建RDD,调用sc及其子类的parallelize方法
        List<Integer> list = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        JavaRDD<Integer> rdd = javaSparkContext.parallelize(list);

        //执行算子操作
        //reduce操纵
        int sum = rdd.reduce(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1+v2;
            }
        });
        //输出结果
        System.out.println("1-10 :"+sum);
        //关闭javaSparkContext
        javaSparkContext.close();
    }
}
