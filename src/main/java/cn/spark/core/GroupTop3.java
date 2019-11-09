package cn.spark.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.PriorityQueue;

public class GroupTop3 {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("SecondarySort").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        JavaRDD<String> lines = sc.textFile("src/score.txt");
        //构造pair
        JavaPairRDD<String,Integer> pairs = lines.mapToPair(
                new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                String[] split = s.split(" ");
                return new Tuple2<>(split[0],Integer.parseInt(split[1]));
            }
        });
        //分组
        JavaPairRDD<String,Iterable<Integer>> groupPairs = pairs.groupByKey();
        //
        JavaPairRDD<String,Iterable<Integer>> top3Scores = groupPairs.mapToPair(
                new PairFunction<Tuple2<String, Iterable<Integer>>, String, Iterable<Integer>>() {
                    @Override
                    public Tuple2<String, Iterable<Integer>> call(Tuple2<String, Iterable<Integer>> t) throws Exception {
                        PriorityQueue<Integer> top3 = new PriorityQueue<>();
                        String className = t._1;
                        Iterable<Integer> scores = t._2;
                        for(Integer n:scores){
                            if (top3.size()<3){
                                top3.add(n);
                            }else if(n>top3.peek()){
                                top3.poll();
                                top3.add(n);
                            }
                        }
                        int[] final3 = new int[3];
                        int sz = top3.size();
                        for(int i=sz-1;i>=0;i--){
                            final3[i]=top3.poll();  //只有poll出来的才是有序的，直接返回top3是无序的存储（堆）
                            //默认是最小堆，12，34，56：要逆序存储
                        }
                        top3=null;
                        return new Tuple2<String, Iterable<Integer>>(className,
                                ()->Arrays.stream(final3).iterator());  //array-->Iterable
                    }
                }
        );

        top3Scores.foreach(new VoidFunction<Tuple2<String, Iterable<Integer>>>() {
            @Override
            public void call(Tuple2<String, Iterable<Integer>> t) throws Exception {
                System.out.print(t._1+" : ");
                for(int i:t._2){  //t._2是Iterable类型
                    System.out.print(i+" ");
                }
                System.out.println();
            }
        });
        sc.close();
    }
}
