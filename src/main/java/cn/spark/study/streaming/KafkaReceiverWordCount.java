package cn.spark.study.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;

public class KafkaReceiverWordCount {
    public static void main(String[] args) throws InterruptedException {
        SparkConf conf = new SparkConf().setAppName("KafkaReceiverWordCount").setMaster("local[2]");
        JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(5));
        //每个topic一个线程去拉取kafka的数据
        HashMap<String, Integer> topicThreadMap = new HashMap<>();
        topicThreadMap.put("WordCount",1);
        //使用KafkaUtitls创建真的kafka数据的输入流
        JavaPairReceiverInputDStream<String, String> lines =
                KafkaUtils.createStream(
                jsc,
                "192.168.137.107:2181,192.168.137.108:2181,192.168.137.109:2181", //zk集群地址
                "DefaultConsumerGroup", //消费者组名任意
                topicThreadMap //拉取的topic名称和线程个数
        );
        //wordCount逻辑
        JavaDStream<String> words = lines.flatMap(new FlatMapFunction<Tuple2<String, String>, String>() {
            @Override
            //tuple的第二个是line文本
            public Iterator<String> call(Tuple2<String, String> t) throws Exception {
                return Arrays.asList(t._2.split(" ")).iterator();
            }
        });

        JavaPairDStream<String,Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<>(s,1);
            }
        });

        JavaPairDStream<String, Integer> wordCounts = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer + integer2;
            }
        });

        wordCounts.print();

        jsc.start();
        jsc.awaitTermination();
        jsc.close();

    }
}

