package cn.spark.study.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.sql.Connection;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * 基于UpdateStateByKey算子实现缓存机制的实时word count
 */
public class PersistWordCount {
    public static void main(String[] args) throws InterruptedException {
        SparkConf conf = new SparkConf().setAppName("PersistWordCount").setMaster("local[2]");
        JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(5));
        jsc.checkpoint("hdfs://spark1:9000/wordcount_checkpoint");
        //wordcount逻辑
        JavaReceiverInputDStream<String> lines = jsc.socketTextStream("localhost", 9999);
        JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) throws Exception {
                return Arrays.asList(s.split(" ")).iterator();
            }
        });

        JavaPairDStream<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<>(s, 1);
            }
        });
        JavaPairDStream<String,Integer> wordcounts =
                pairs.updateStateByKey(new Function2<List<Integer>, Optional<Integer>, Optional<Integer>>() {
            @Override
            public Optional<Integer> call(List<Integer> values, Optional<Integer> state)
                    throws Exception {
                //全局单词计数
                Integer newVal = 0;
                //判断key之前是否出现过
                if(state.isPresent()){
                    newVal = state.get();
                }
                //接着累加本次的values
                for(int i:values){
                    newVal+=i;
                }
                return Optional.of(newVal);
            }
        });

        //得到全局统计的次数后，将其写入mysql
        wordcounts.foreachRDD(new VoidFunction<JavaPairRDD<String, Integer>>() {
            @Override
            public void call(JavaPairRDD<String, Integer> wordCountsRDD) throws Exception {
                //调用RDD的foreachPartition()
                wordCountsRDD.foreachPartition(new VoidFunction<Iterator<Tuple2<String, Integer>>>() {
                    @Override
                    public void call(Iterator<Tuple2<String, Integer>> wordCounts) throws Exception {
                        //每个partition获取连接
                        Connection con = ConnectionPool.getConnection();
                        Tuple2<String,Integer> wordCount = null;
                        //遍历partition的数据，写入mysql
                        while (wordCounts.hasNext()){
                            wordCount = wordCounts.next();
                            String sql = "insert into wordcount(word,count) values('"+wordCount._1+"',"+wordCount._2+")";
                            Statement stmt = con.createStatement();
                            stmt.executeUpdate(sql);

                        }
                        ConnectionPool.returnConnection(con);
                    }
                });
            }
        });

        jsc.start();
        jsc.awaitTermination();
        jsc.close();
    }
}
