package cn.spark.study.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.List;

/**
 * 滑动窗口热点搜索词统计
 */
public class WindowHotWord {
    public static void main(String[] args) throws InterruptedException {
        SparkConf conf = new SparkConf().setAppName("WindowHotWord").setMaster("local[2]");
        JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(5));

        //搜索日志：leo hello
        JavaReceiverInputDStream<String> searchLogDStream = jsc.socketTextStream("spark1", 9999);

        JavaDStream<String> searchWordDStream = searchLogDStream.map(new Function<String, String>() {
            @Override
            public String call(String s) throws Exception {
                return s.split(" ")[1];
            }
        });

        JavaPairDStream<String, Integer> searchWordPairsDStream = searchWordDStream.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<>(s, 1);
            }
        });
        //针对(searchWord,1)格式针对60s的window进行计算，隔10秒出一个窗口
        JavaPairDStream<String, Integer> searchWordCountsDStream = searchWordPairsDStream.reduceByKeyAndWindow(
                new Function2<Integer, Integer, Integer>() {
                    @Override
                    public Integer call(Integer integer, Integer integer2) throws Exception {
                        return integer + integer2;
                    }
                },
                Durations.seconds(60),//窗口长度60s，每隔10秒把前60秒的数据(12个batch = 60s/5s)计算一次
                Durations.seconds(10)//滑动间隔时间
        );

        //一个窗口一个60s的数据，会称为一个新的RDD，对这个rdd根据每个搜索词出现的频率排序，获取tops
        JavaPairDStream<String, Integer> finalDStream = searchWordCountsDStream.transformToPair(
                new Function<JavaPairRDD<String, Integer>, JavaPairRDD<String, Integer>>() {
                    @Override
                    public JavaPairRDD<String, Integer> call(JavaPairRDD<String, Integer> searchWordCountsRDD) throws Exception {

                        //搜索次出频次的反转
                        JavaPairRDD<Integer, String> countSearchWordRDD = searchWordCountsRDD.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
                            @Override
                            public Tuple2<Integer, String> call(Tuple2<String, Integer> t) throws Exception {
                                return new Tuple2<>(t._2, t._1);
                            }
                        });
                        //count排序
                        JavaPairRDD<Integer, String> sortedCountWordRDD = countSearchWordRDD.sortByKey(false);
                        //再次反转
                        JavaPairRDD<String, Integer> sortedSearchWordRDD = sortedCountWordRDD.mapToPair(new PairFunction<Tuple2<Integer, String>, String, Integer>() {
                            @Override
                            public Tuple2<String, Integer> call(Tuple2<Integer, String> t) throws Exception {
                                return new Tuple2<>(t._2, t._1);
                            }
                        });
                        //取top3
                        List<Tuple2<String, Integer>> hotSearchWordCounts = sortedSearchWordRDD.take(3);
                        for(Tuple2<String,Integer> t:hotSearchWordCounts){
                            System.out.println(t);
                        }

                        return sortedSearchWordRDD;
                    }

                });

        finalDStream.print();

        jsc.start();
        jsc.awaitTermination();
        jsc.close();
    }
}
