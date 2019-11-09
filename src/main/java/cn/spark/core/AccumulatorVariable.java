package cn.spark.core;

import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

import java.util.Arrays;
import java.util.List;

public class AccumulatorVariable {

    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("AccumulatorVariable").setMaster("local");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);

        Accumulator<Integer> sum = javaSparkContext.intAccumulator(0);
        List<Integer> nuList = Arrays.asList(1, 2, 3, 4, 5);
        JavaRDD<Integer> nums = javaSparkContext.parallelize(nuList);
        nums.foreach(new VoidFunction<Integer>() {
            @Override
            public void call(Integer integer) throws Exception {
                sum.add(integer);
            }
        });
        //Driver可以读取accumulator
        System.out.println(sum.value());
        javaSparkContext.close();
    }
}
