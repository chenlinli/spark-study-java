package cn.spark.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

/**
 * 集群部署cn.spark.core.WordCountCluster
 * 1.删除setMaster(),默认自己会连接
 * 2.文件修改：hdfs上的文件
 */
public class WordCountCluster {
    public static void main(String[] args){

        //执行步骤：上传spark.txt到hdfs
        //mvn 打包上传到spark1
        //编写spark0submit脚本
        //执行spark-submit脚本，提交到集群执行
        SparkConf sparkConf = new SparkConf()
                .setAppName("WordCountLocal");

        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);

        JavaRDD<String> lines = javaSparkContext.textFile("hdfs://spark1:9000/spark.txt");

        JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String line) throws Exception {
                return Arrays.asList(line.split(" ")).iterator();
            }
        });

        JavaPairRDD<String,Integer> pairRDD = words.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String word) throws Exception {
                return new Tuple2<String,Integer>(word,1);
            }
        });

        JavaPairRDD<String,Integer> wordCounts = pairRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1+v2;
            }
        });

        wordCounts.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            @Override
            public void call(Tuple2<String, Integer> wordCount) throws Exception {
                System.out.println(wordCount._1+" :  "+wordCount._2);
            }
        });

    }
}
