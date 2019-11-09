package cn.spark.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

/**
 * 实现自定义key,实现Ordered,Serializeable，key实现排序规则
 * 根据源RDD的每一行构造自定义的key,其vaule是文本行，这就是JavaPairRDD
 * 对上述JavaPairRDD  sortByKey
 * //
 */
public class SecondarySort {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("SecondarySort").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        JavaRDD<String> lines = sc.textFile("src/sort.txt");

        //最终是要根据SecondarySortKey排序的，根据line构造SecondarySortKey
        JavaPairRDD<SecondarySortKey, String> pairs = lines.mapToPair(new PairFunction<String, SecondarySortKey, String>() {
            @Override
            public Tuple2<SecondarySortKey, String> call(String line) throws Exception {
                String[] split = line.split(" ");
                return new Tuple2<>(new SecondarySortKey(Integer.parseInt(split[0]),Integer.parseInt(split[1]))
                        ,line);
            }
        });
        JavaPairRDD<SecondarySortKey, String> sortedPairs = pairs.sortByKey();
        //不需要key，之后需要value
        JavaRDD<String> sortedLines = sortedPairs.map(new Function<Tuple2<SecondarySortKey, String>, String>() {
            @Override
            public String call(Tuple2<SecondarySortKey, String> secondarySortKeyStringTuple2) throws Exception {
                return secondarySortKeyStringTuple2._2;
            }
        });

        sortedLines.foreach(new VoidFunction<String>() {
            @Override
            public void call(String s) throws Exception {
                System.out.println(s);
            }
        });

        sc.close();

    }
}
