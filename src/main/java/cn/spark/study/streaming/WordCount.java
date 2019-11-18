package cn.spark.study.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

/**
 * 实时word count
 */
public class WordCount {
    public static void main(String[] args) throws InterruptedException {
        //创建Sparkconf:设置master属性，但是测试时使用local模式，local后跟一个"[]"，“[]”里写一个数字
        // ，数字代表用几个线程来执行程序
        SparkConf conf = new SparkConf().setAppName("WordCount")
                .setMaster("local[2]");
        //创建JavaStramingContext对象，类似于Spark core里的JavaSparkContext
        //Durations:batch interval:batch收集的时间，这里是1s,1s产生一个batch
        JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(1));

        //创建输入DStream，代表以过热从数据源来的持续不断的实时数据流
        //调用JavaStreamingContext的socketTextStream可以创建一个数据源为socket网络端口的数据流
        //socketTextStream:监听的主机，监听的端口
        JavaReceiverInputDStream<String> lines = jsc.socketTextStream("localhost", 9999);
        //这里，可以认为JavaReceiverInputDStream里每隔1s会产生一个RDD，封装了这一秒发过来的数据，R
        //JavaReceiverInputDStream<String> 代表底层RDD的泛型类型String
        // RDD类型为String，一行行文本

        //开始计算RDD，使用core的算子应用在DStreaam即可:实际上是对DStream里的RDD计算，产生的新的RDD,
        //作为新的DStream里的RDD
        JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) throws Exception {
                return Arrays.asList(s.split(" ")).iterator();
            }
        });
        //每秒里收集的一行行文本就会被拆成单词
        JavaPairDStream<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<>(s, 1);
            }
        });
        //用Spark Streaming和Spark core很相似，
        // 不同在于，JavaRDD-->JavaDStream;JavaPairRDD-->JavaPairDStream
        JavaPairDStream<String, Integer> wordCounts = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer + integer2;
            }
        });
        //到此wordCount结束
        //Streamig的计算模型必须自己进行中间缓存的控制，与Storm完全不同，Storm可以在对象里控制缓存
        //Spark是函数式编程的计算模型，所以没法在实例变量里缓存，只能将最后计算的一个个RDD,昔日他外部缓存/持久化
        ////最后每次计算完都打印，这一秒的单词情况
        Thread.sleep(5000);
        wordCounts.print();

        //对JavaStreamingContext处理
        jsc.start();//必须调用start(),app才能执行
        jsc.awaitTermination();//等待结束运行
        jsc.close();
    }
}
