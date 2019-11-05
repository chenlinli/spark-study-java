package cn.spark.core;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class ActionOperation {

    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("ActionOperation")
                .setMaster("local")
                ;
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
//        reduce(javaSparkContext);
//        collect(javaSparkContext);
//        count(javaSparkContext);
//        take(javaSparkContext);
//        saveAsTextFile(javaSparkContext);
        countByKey(javaSparkContext);
        javaSparkContext.close();

    }

    public static void reduce(JavaSparkContext sc){
        //集合求和
        List<Integer> numberList = Arrays.asList(1, 2, 3, 5, 6, 7, 8, 9, 10);
        JavaRDD<Integer> numbers = sc.parallelize(numberList);
        //reduce第一个和第二个元素传入call,返回的结果与下一个元素传入call,计算……
        //聚会计算
        Integer sum = numbers.reduce(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer + integer2;
            }
        });
        System.out.println(sum);
    }

    public static void collect(JavaSparkContext sc){
        //集合求和
        List<Integer> numberList = Arrays.asList(1, 2, 3, 5, 6, 7, 8, 9, 10);
        JavaRDD<Integer> numbers = sc.parallelize(numberList);
        //
        JavaRDD<Integer> doubleNums = numbers.map(new Function<Integer, Integer>() {
            @Override
            public Integer call(Integer integer) throws Exception {
                return integer * 2;
            }
        });

        //不用foreach遍历集群的rdd，使用collect:将集群的rdd拉取到本地
        //不建议，数据了大，耗带宽和内存
        List<Integer> doubleNumList = doubleNums.collect();
        for (Integer num:doubleNumList) {
            System.out.println(num);
        }

    }

    public static void count(JavaSparkContext sc){
        //集合求和
        List<Integer> numberList = Arrays.asList(1, 2, 3, 5, 6, 7, 8, 9, 10);
        JavaRDD<Integer> numbers = sc.parallelize(numberList);
        ///统计元素个数
        long count = numbers.count();
        System.out.println(count);

    }

    public static void take(JavaSparkContext sc){
        List<Integer> numberList = Arrays.asList(1, 2, 3, 5, 6, 7, 8, 9, 10);
        JavaRDD<Integer> numbers = sc.parallelize(numberList);
        //take:与collect类似，远程数据获取到本地，collect获取所有数据，take获取前n个
        List<Integer> top3Num = numbers.take(3);

        for(Integer n:top3Num){
            System.out.println(n);
        }
    }
    public static void saveAsTextFile(JavaSparkContext sc){
        List<Integer> numberList = Arrays.asList(1, 2, 3, 5, 6, 7, 8, 9, 10);
        JavaRDD<Integer> numbers = sc.parallelize(numberList);
        //
        JavaRDD<Integer> doubleNums = numbers.map(new Function<Integer, Integer>() {
            @Override
            public Integer call(Integer integer) throws Exception {
                return integer * 2;
            }
        });
        //rdd数据直接保存在文件里
        //只能是文件夹：真实内容在文件夹下的part-00000里
        doubleNums.saveAsTextFile("hdfs://spark1:9000/double_num");
    }

    public static void countByKey(JavaSparkContext sc){
        List<Tuple2<String,Integer>> scoreList = Arrays.asList(
                new Tuple2<>("c1",60),
                new Tuple2<>("c2",90),
                new Tuple2<>("c1",70),
                new Tuple2<>("c3",50),
                new Tuple2<>("c3",67),
                new Tuple2<>("c1",80)
        );
        JavaPairRDD<String,Integer> stus = sc.parallelizePairs(scoreList);
        //countByKey统计key对应元素的个数
        Map<String, Long> stuCounts = stus.countByKey();

        for(Map.Entry<String,Long> entry:stuCounts.entrySet()){
            System.out.println(entry.getKey()+":"+entry.getValue());
        }
    }

    public static void foreach(JavaSparkContext sc){
    }
}
