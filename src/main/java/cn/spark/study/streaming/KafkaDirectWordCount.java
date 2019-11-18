package cn.spark.study.streaming;

import kafka.serializer.StringDecoder;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;

public class KafkaDirectWordCount {
    public static void main(String[] args) throws InterruptedException {
        SparkConf conf = new SparkConf().setAppName("KafkaReceiverWordCount").setMaster("local[2]");
        JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(5));

        //创建Kafka参数：
        //每个topic一个线程去拉取kafka的数据
        HashMap<String, String> kafkaParams = new HashMap<>();
        kafkaParams.put("metadata.broker.list",
                "192.168.137.107:9092,192.168.137.108:9092,192.168.137.109:9092");
        //创建set:放入读取的topic，可以并行读取多个topic
        HashSet<String> topics = new HashSet<>();
        topics.add("WordCount");

        JavaPairInputDStream<String, String> lines = KafkaUtils.createDirectStream(
                jsc,
                String.class,//JavaPairInputDStream：第一个泛型key类型
                String.class,//JavaPairInputDStream：value的类型
                StringDecoder.class, //类型的解码器
                StringDecoder.class,
                kafkaParams,
                topics);
        JavaDStream<String> words = lines.flatMap(new FlatMapFunction<Tuple2<String, String>, String>() {
            @Override
            public Iterator<String> call(Tuple2<String, String> t) throws Exception {
                return Arrays.asList(t._2.split(" ")).iterator();
            }
        });

        JavaPairDStream<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<>(s, 1);
            }
        });

        JavaPairDStream<String, Integer> wordcounts = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer + integer2;
            }
        });

        wordcounts.print();

        jsc.start();
        jsc.awaitTermination();
        jsc.close();


    }
}
