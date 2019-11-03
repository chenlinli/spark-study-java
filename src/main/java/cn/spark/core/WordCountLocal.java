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
import java.util.List;

/**
 * 本地测试
 */
public class WordCountLocal {
    public static void main(String[] args){
        //编写spark
        //创建SparkConf 设置配置信息
        //seMaster设置Spark集群master的url,如果设置为local,则代表本地运行
        SparkConf sparkConf = new SparkConf()
                .setAppName("WordCountLocal")
                .setMaster("local");

        //创建JavaSparkContext对象
        //Spark里sparkContext是Spark所有功能的入口，无论java/python编写
        //都要求一个sparkcontext,包括初始化spark应用程序所需的一些核心组件，
        //调度器，到spark master注册，等
        //sparkcontext是spark应用最重要的一个对象
        //不同语言编写使用的sparkcontext是不同的
        //scala:原生SparkContext
        //java:JavaSarkContext
        //spark sql:SQLContext/HiveContext
        //spark streaming:它独有的SparkContext对象
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);

        //针对输入源（hdfs,本地文件…… ）创建初始RDD
        //输入源的数据会打散，分配到RDD的每个partition里
        //这里针对本地文件
        //SparkContext里用于根据文件类型输入源创建RDD的方法：textFile()
        //java中创建的普通RDD,JavaRDD
        //这里RDD有元素的概念，如果是HDFS/本地文件创建的RDD,每个元素就是文件的一行
        JavaRDD<String> lines = javaSparkContext.textFile("src/spark.txt");

        //初始RDD,transformation操作：计算操作,通过创建Function并配合RDD的map，flatMp等算子
        //操作简单：创建匿名内部类，复杂则创建单独的类
        //FlatMapFunction：输入肯定是String（一行文本）,输出也是String：将RDD的一个元素拆分成一个或多个元素
        //将每一行拆分成单词
        JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String line) throws Exception {
                return Arrays.asList(line.split(" ")).iterator();
            }
        });

        //每个单词映射为（word,1）的格式，只有这样才能后面把word作为key做单词数目的累加
        //mapToPair:将每个元素映射为(v1,v2)Tuple2类型的元素
        //Tuple2是scala类型
        //mapToPair：要求与PairFunction配合使用：<输入类型，Tuple2第一个值类型，Tuple2第二个值类型>
        //JavaPairRDD<String,Integer>:分别代表Tuple2第一、二个元素类型（String,Integer）
        JavaPairRDD<String,Integer> pairRDD = words.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String word) throws Exception {
                return new Tuple2<String,Integer>(word,1);
            }
        });

        //以单词为key，统计个数
        //使用reduceBykey:对每个key的value进行reduce操作
        //JavaPairRDD:（“hello,1”）,("hello",1),("cm",1)
        //reduce：第一个和第二个计算，第三个和前面计算的结果计算
        //最后返回JavaPairRDD<String,Integer>:Tuple2:key,每个key reduce的结果（count）
        JavaPairRDD<String,Integer> wordCounts = pairRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1+v2;
            }
        });

        //算出结果
        //前面都是transformation操作，必须最后使用actor操作如foreach:遍历每个Tuple2
        wordCounts.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            @Override
            public void call(Tuple2<String, Integer> wordCount) throws Exception {
                System.out.println(wordCount._1+" :  "+wordCount._2);
            }
        });

    }
}
