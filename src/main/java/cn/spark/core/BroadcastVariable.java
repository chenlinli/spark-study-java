package cn.spark.core;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;

import java.util.Arrays;
import java.util.List;

public class BroadcastVariable {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("BroadcastVariable").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        int factor = 3;
        //共享变量
        Broadcast<Integer> factorBroadcast = sc.broadcast(factor);

        List<Integer> nuList = Arrays.asList(1, 2, 3, 4, 5);
        JavaRDD<Integer> numbers = sc.parallelize(nuList);

        //每个数字乘外部定义的factor
        JavaRDD<Integer> multipliedNumbers = numbers.map(new Function<Integer, Integer>() {
            @Override
            public Integer call(Integer integer) throws Exception {
                return integer * factor;
            }
        });
        multipliedNumbers.foreach(new VoidFunction<Integer>() {
            @Override
            public void call(Integer integer) throws Exception {
                int factor = factorBroadcast.value();//内部封装值
                System.out.println(integer);
            }
        });

        sc.close();
    }
}
