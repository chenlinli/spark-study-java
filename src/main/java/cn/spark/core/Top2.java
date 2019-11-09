package cn.spark.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.List;

public class Top2 {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("SecondarySort").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        JavaRDD<String> lines = sc.textFile("src/top.txt");
        JavaPairRDD<Integer,Integer> numbers = lines.mapToPair(new PairFunction<String, Integer, Integer>() {
            @Override
            public Tuple2<Integer, Integer> call(String s) throws Exception {
                return new Tuple2<>(Integer.parseInt(s),1);
            }
        });
        JavaPairRDD<Integer, Integer> sortNums = numbers.sortByKey(false);
        JavaRDD<Integer> sorted = sortNums.map(new Function<Tuple2<Integer, Integer>, Integer>() {
            @Override
            public Integer call(Tuple2<Integer, Integer> t) throws Exception {
                return t._1;
            }
        });

        List<Integer> list = sorted.take(3);
        for(int n:list){
            System.out.println(n);
        }

        sc.close();
    }
}
