package cn.spark.study.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * 基于UpdateStateByKey算子实现缓存机制的实时word count
 */
public class UpdateStateByKeyWordCount {
    public static void main(String[] args) throws InterruptedException {
        SparkConf conf = new SparkConf().setAppName("UpdateStateByKeyWordCount").setMaster("local[2]");
        JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(5));

        //要使用UpdateStateByKey算子就必须有checkpoint目录，开启checkpoint机制
        //才能把每个key对应的state在内存里有，需要checkpoint,一份，因为需要长期保存state
        //便于内存数据丢失后，可以从checkpoint中恢复数据
        //开启checkpoint:设置目录
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
        //这里就不一样了
        //之前pairs.reduceByKey：就可以得到一个时间段的单词计数，
        // 但是如果需要统计从程序启动开始到现在为止一个单词出现的次数，之前的方式则无法解决
        //那么需要基于redis缓存/mysql这种db来累加
        //但是，这里可以通过spark维护一份全局单词统计次数
        JavaPairDStream<String,Integer> wordcounts =
                pairs.updateStateByKey(new Function2<List<Integer>, Optional<Integer>, Optional<Integer>>() {
            @Override
            //对于每个单词，每次batch计算时，都会调用call()
            //values：这个batch里，key放入新的值可能有两个，如(hello,1),(hello,1)那么values就是(1,2)
            // Optional:可以理解为scala的样例类，代表key之前的的状态，存在/不存在，其中的泛型是自己执行的
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

        //这里每个batch过来时，计算到pairs DStream就会执行全局的updateStateByKey算子，
        //updateStateByKey返回的JavaPairDStream就代表了每个key的全局计数
        wordcounts.print();

        jsc.start();
        jsc.awaitTermination();
        jsc.close();
    }
}
