package cn.spark.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class TransformationOperation {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName(""
        ).setMaster("local");

        //sc
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        //map(javaSparkContext);
        //filter(javaSparkContext);
        //flatMap(javaSparkContext);
        //groupByKey(javaSparkContext);
//        reduceByKey(javaSparkContext);
//        sortByKey(javaSparkContext);
        join(javaSparkContext);
        //coGroup(javaSparkContext);
        javaSparkContext.close();
    }

    /**
     * 集合每个元素乘2
     */
    private static void map(JavaSparkContext sc){
        //创建集合
        List<Integer> list = Arrays.asList(1, 2, 3, 4, 5, 6);
        //创建初始并行RDD
        JavaRDD<Integer> numsRdd = sc.parallelize(list);
        //使用map：将集合的每个元素乘2,接收Function
        //Function一定让设置第二个泛型：返回的新元素类型（call的返回泛型）
        //call内部对原始rdd的每个元素操作，返回新的元素，所有新元素组成一个新RDD
        JavaRDD<Integer> multipliedNumberRdd = numsRdd.map(new Function<Integer, Integer>() {
            @Override
            public Integer call(Integer integer) throws Exception {
                return integer*2;
            }
        });

        //打印
        multipliedNumberRdd.foreach(new VoidFunction<Integer>() {
            @Override
            public void call(Integer integer) throws Exception {
                System.out.println(integer);
            }
        });

    }

    private static void filter(JavaSparkContext sc){
        List<Integer> numbers = Arrays.asList(1, 2, 3, 4, 5, 6);

        JavaRDD<Integer> numbersRDD = sc.parallelize(numbers);
        //filter过滤出偶数:传入Function,实际上和map一样，只是call返回是Boolean
        //每个元素都会传入call，call中过滤出想要的元素，返回true保留该元素，false：丢弃该元素
        //new Function<Integer, Boolean>:<输入的类型，过滤条件>
        JavaRDD<Integer> evenNumberRDD = numbersRDD.filter(new Function<Integer, Boolean>() {
            @Override
            public Boolean call(Integer integer) throws Exception {
                return integer % 2 == 0;
            }
        });
         evenNumberRDD.foreach(new VoidFunction<Integer>() {
             @Override
             public void call(Integer integer) throws Exception {
                 System.out.println(integer);
             }
         });
    }

    /**
     * 一行拆分单词
     * @param sc
     */
    private static void flatMap(JavaSparkContext sc){
        //集合
        List<String> lineList = Arrays.asList("hello you", "hello me", "hello him", "hello world");
        JavaRDD<String> lineRDD = sc.parallelize(lineList);
        //flatMap：拆分单词
        //flatMap接收FlatMapFunction<String,T>:第二个泛型返回的新元素类型
        //call返回的不是T,而是Iterable<T>
        //flatMap接收原始RDD的每个元素，帮进行各种逻辑处理，可以返回多个元素，封装在Iterable集合里
        //新RDD中封装了所有新元素，新RDD一个顶大于旧RDD
        JavaRDD<String> words = lineRDD.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) throws Exception {
                return Arrays.asList(s.split(" ")).iterator();
            }
        });

        words.foreach(new VoidFunction<String>() {
            @Override
            public void call(String s) throws Exception {
                System.out.println(s);
            }
        });
    }

    /**
     * 按照班级分组成绩
     * @param sc
     */
    private static void groupByKey(JavaSparkContext sc){
        List<Tuple2<String,Integer>> scoreList = Arrays.asList(
                new Tuple2<>("c1",80),
                new Tuple2<>("c2",90),
                new Tuple2<>("c1",70),
                new Tuple2<>("c3",50),
                new Tuple2<>("c3",67),
                new Tuple2<>("c1",80)
          );

        JavaPairRDD<String,Integer> scores = sc.parallelizePairs(scoreList);
        //针对rdd，执行groupByKey对班级成绩分组
        //反回JavaPairRDD：第一个泛型不变（输入类型），第二个泛型是Iterable<第一个泛型>
        //每个key,可能有多个值
        JavaPairRDD<String, Iterable<Integer>> groupedScores = scores.groupByKey();

        groupedScores.foreach(new VoidFunction<Tuple2<String, Iterable<Integer>>>() {
            @Override
            public void call(Tuple2<String, Iterable<Integer>> t) throws Exception {
                System.out.print("class:"+t._1+" ");
                Iterator<Integer> it = t._2.iterator();
                while (it.hasNext()){
                    System.out.print(it.next()+" ");
                }
                System.out.println();
            }
        });
    }

    /**
     * 统计班级总分
     * @param sc
     */
    private static void reduceByKey(JavaSparkContext sc){
        List<Tuple2<String,Integer>> scoreList = Arrays.asList(
                new Tuple2<>("c1",80),
                new Tuple2<>("c2",90),
                new Tuple2<>("c1",70),
                new Tuple2<>("c3",50),
                new Tuple2<>("c3",67),
                new Tuple2<>("c1",80)
        );
        JavaPairRDD<String,Integer> scores = sc.parallelizePairs(scoreList);
        //new Function2<Integer, Integer, Integer> :<元素PairRDD的第一个类型（key type），
        // PairRDD的第二个类型（value type），每次reduce返回值的类型（默认与）value type相同>
        //reduceByKeymore返回的是JavaPairRDD<key type,value type>
        JavaPairRDD<String, Integer> totalScores = scores.reduceByKey(new Function2<Integer, Integer, Integer>() {
            //每个key都会传入call，从而聚合成一个value，将key和集合的value组成一个Tuple2
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer+integer2;
            }
        });

        totalScores.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            @Override
            public void call(Tuple2<String, Integer> classScore) throws Exception {
                System.out.println(classScore._1+ " : "+classScore._2);
            }
        });
    }

    /**
     * 学生分数排序
     * @param sc
     */
    private static void sortByKey(JavaSparkContext sc) {
        List<Tuple2<Integer, String>> scoreList = Arrays.asList(new Tuple2<Integer, String>(56, "cc"), new Tuple2<Integer, String>(67, "ct"),
                new Tuple2<Integer, String>(99, "tom"),new Tuple2<Integer, String>(88, "marry"));
        JavaPairRDD<Integer, String> stuSocres = sc.parallelizePairs(scoreList);

        //参数true:升序，false降序
        JavaPairRDD<Integer, String> sortedScores = stuSocres.sortByKey(false);
        sortedScores.foreach(new VoidFunction<Tuple2<Integer, String>>() {
            @Override
            public void call(Tuple2<Integer, String> scoreStu) throws Exception {
                System.out.println(scoreStu._1+":"+scoreStu._2);
            }
        });
    }

    /**
     * 打印学生成绩：
     * @param sc
     */
    private static void join(JavaSparkContext sc) {
        List<Tuple2<Integer,String>> stuList = Arrays.asList(
                new Tuple2<Integer, String>(1,"leo"),
                new Tuple2<Integer, String>(2,"jack"),
                new Tuple2<Integer, String>(3,"tom")
        );
//        List<Tuple2<Integer,Integer>> scoreList = Arrays.asList(
//                new Tuple2<Integer, Integer>(1,100),
//                new Tuple2<Integer, Integer>(2,77),
//                new Tuple2<Integer, Integer>(3,88)
//        );
        List<Tuple2<Integer,Integer>> scoreList = Arrays.asList(
                new Tuple2<Integer, Integer>(1,100),
                new Tuple2<Integer, Integer>(2,77),
                new Tuple2<Integer, Integer>(3,88),
                new Tuple2<Integer, Integer>(3,50),
                new Tuple2<Integer, Integer>(2,89),
                new Tuple2<Integer, Integer>(1,78)
        );
        //并行化两个集合
        JavaPairRDD<Integer, String> stus = sc.parallelizePairs(stuList);
        JavaPairRDD<Integer, Integer> scores = sc.parallelizePairs(scoreList);

        //join关联2个rdd:根据key,join返回JavaPairRDD
        //<Integer：两个RDD的key（根据key join的）, Tuple2<String, Integer>:两个RDD的value的类型，第一个是join的主动方，第二个是被join的>
        JavaPairRDD<Integer, Tuple2<String, Integer>> stuScore = stus.join(scores);
        stuScore.foreach(new VoidFunction<Tuple2<Integer, Tuple2<String, Integer>>>() {
            @Override
            public void call(Tuple2<Integer, Tuple2<String, Integer>> idStuScore) throws Exception {
                System.out.println(idStuScore._1+":<"+idStuScore._2._1+","+idStuScore._2._2+">");
                /**
                 1:<leo,100>
                 1:<leo,78>
                 3:<tom,88>
                 3:<tom,50>
                 2:<jack,77>
                 2:<jack,89>
                 */
            }
        });
    }

    private static void coGroup(JavaSparkContext sc) {
        List<Tuple2<Integer,String>> stuList = Arrays.asList(
                new Tuple2<Integer, String>(1,"leo"),
                new Tuple2<Integer, String>(2,"jack"),
                new Tuple2<Integer, String>(3,"tom")
        );
        List<Tuple2<Integer,Integer>> scoreList = Arrays.asList(
                new Tuple2<Integer, Integer>(1,100),
                new Tuple2<Integer, Integer>(2,77),
                new Tuple2<Integer, Integer>(3,88),
                new Tuple2<Integer, Integer>(3,50),
                new Tuple2<Integer, Integer>(2,89),
                new Tuple2<Integer, Integer>(1,78)
        );
        //并行化两个集合
        //相当于一个key join上的所有value都放到一个Iterable里
        JavaPairRDD<Integer, String> stus = sc.parallelizePairs(stuList);
        JavaPairRDD<Integer, Integer> scores = sc.parallelizePairs(scoreList);

        JavaPairRDD<Integer, Tuple2<Iterable<String>, Iterable<Integer>>> stuScore = stus.cogroup(scores);

        stuScore.foreach(new VoidFunction<Tuple2<Integer, Tuple2<Iterable<String>, Iterable<Integer>>>>() {
            @Override
            public void call(Tuple2<Integer, Tuple2<Iterable<String>, Iterable<Integer>>> t) throws Exception {
                System.out.println(t._1+":<"+t._2._1+t._2._2+">");
                /**
                 1:<[leo][100, 78]>
                 3:<[tom][88, 50]>
                 2:<[jack][77, 89]>
                 */
            }
        });
    }

}
