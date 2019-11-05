package cn.spark.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

/**
 * 统计每行出现的次数
 */
public class LineCount {

    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName(""
        ).setMaster("local");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);

        JavaRDD<String> rdd = javaSparkContext.textFile("src/hello.txt");
        //每行文本运算-->(line,1):String（输入元素类型）, String（key类型）, Integer(value类型)
        JavaPairRDD<String, Integer> pairs = rdd.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String line) throws Exception {
                return new Tuple2<String, Integer>(line,1);
            }
        });

        //reduceByKey:根据key收集
        JavaPairRDD<String, Integer> lineCounts = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        lineCounts.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            @Override
            public void call(Tuple2<String, Integer> lineCount) throws Exception {
                System.out.println(lineCount._1+" : "+lineCount._2);
            }
        });

    }
}
