package cn.spark.study.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class TransformBlackList {
    public static void main(String[] args) throws InterruptedException {
        SparkConf conf = new SparkConf().setAppName("TransformBlackList").setMaster("local[2]");
        JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(5));

        //用户对网站的广告可点击，进行实时计费，点击一下算一次钱
        //对于刷广告的人，黑名单的用户点击广告过滤掉
        //黑名单rdd
        List<Tuple2<String,Boolean>> blackListData = new ArrayList<>();
        blackListData.add(new Tuple2<>("tom",true));

        final JavaPairRDD<String,Boolean> blackListRDD = jsc.sparkContext().parallelizePairs(blackListData);
        //log:date username
        JavaReceiverInputDStream<String> addClikLog = jsc.socketTextStream("spark1", 9999);
        //转换数据格式：username date_username，便于对每个batch rdd与定义号的黑名单rdd join
        JavaPairDStream<String, String> userAsdClikLogDStream = addClikLog.mapToPair(new PairFunction<String, String, String>() {
            @Override
            public Tuple2<String, String> call(String s) throws Exception {
                return new Tuple2<String, String>(s.split(" ")[1],s);
            }
        });

        //transform：将DStream的每个batch rdd与黑名单JavaPairRDD 进行操作，过滤
        JavaDStream<String> validAdsClikLogDStream = userAsdClikLogDStream.transform(
                new Function<JavaPairRDD<String, String>, JavaRDD<String>>() {
                    @Override
                    public JavaRDD<String> call(JavaPairRDD<String, String> userAdsClikLogRDD) throws Exception {
                        //左外连接，并不是每个用户都在黑名单，如果只是join,那么黑名单里没有的用户信息就会丢失
                        //左外链接会保存userAdsClikLogRDD里和blackListRDD匹配的和不匹配的
                        //<String：username,
                        // Tuple2<String：日志,
                        // Optional<Boolean>：blackList的第二个泛型，用户是否在黑名单里,因为左外连接，所以可能用户不在blackList里，
                        // 所以这个字段可能不存在>>
                        JavaPairRDD<String, Tuple2<String, Optional<Boolean>>> joinedRDD = userAdsClikLogRDD.leftOuterJoin(blackListRDD);

                        //过虑操作
                        JavaPairRDD<String, Tuple2<String, Optional<Boolean>>> filteredRDD = joinedRDD.filter(
                                new Function<Tuple2<String,
                                        Tuple2<String, Optional<Boolean>>>, Boolean>() {
                                    @Override
                                    public Boolean call(Tuple2<String,
                                            Tuple2<String, Optional<Boolean>>> tuple)
                                    //tuple：log和blackListRDD join后的每一行
                                            throws Exception {
                                        if (tuple._2._2.isPresent() && tuple._2._2.get())
                                            return false; //黑名单过滤掉
                                        return true;//不是黑名单，保留
                                    }
                                });

                        //filteredRDD里只剩合法的用户
                        //进行map,转换格式
                        JavaRDD<String> validClickLogsRDD = filteredRDD.map(new Function<Tuple2<String, Tuple2<String, Optional<Boolean>>>, String>() {
                            @Override
                            public String call(Tuple2<String, Tuple2<String, Optional<Boolean>>> tuple) throws Exception {
                                return tuple._2._1;
                            }
                        });
                        return validClickLogsRDD;

                    }
                });

        //打印有效广告点击日志
        //真实就可以写入kafka中间件消息队列，开发专门后台服务
        validAdsClikLogDStream.print();


        jsc.start();
        jsc.awaitTermination();
        jsc.close();

    }
}
