package cn.spark.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

/**
 * HDFS文件创建Rdd
 */
public class HDFSFile {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("LocalFile");
        JavaSparkContext javaSparkContext = new JavaSparkContext(conf);

        //针对本地文件创建rdd，testFile
        JavaRDD<String> lines = javaSparkContext.textFile("hdfs://spark1:9000/spark.txt");

        //统计文件字数
        JavaRDD<Integer> lineLength = lines.map(new Function<String, Integer>() {
            @Override
            public Integer call(String s) throws Exception {
                return s.length();
            }
        });

        Integer count = lineLength.reduce(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        System.out.println("文件总字数："+count);
        //关闭sc
        javaSparkContext.close();

    }
}
