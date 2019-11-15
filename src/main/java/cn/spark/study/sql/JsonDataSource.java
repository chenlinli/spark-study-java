package cn.spark.study.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class JsonDataSource {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("JsonDataSource");
        JavaSparkContext sc = new JavaSparkContext(conf);

        SQLContext sqlContext = new SQLContext(sc);
        //针对json文件-->DF
        Dataset<Row> studentsScoreDF = sqlContext.read().json("hdfs://spark1:9000/students.json");

        //注册临时表，对临时表做查询
        studentsScoreDF.createOrReplaceTempView("student_scores");
        Dataset<Row> goodStudentScoresDF = sqlContext
                .sql("select name,score from student_scores where score >= 80");

        //df--RDd，transformation操作
        List<String> goodStudentsNames = goodStudentScoresDF.javaRDD().map(new Function<Row, String>() {
            @Override
            public String call(Row row) throws Exception {
                return row.getString(0);
            }
        }).collect();

        //然后针对包含json串的JavaRDD<String> 创建RDD
        List<String> studentInfosJSONs = new ArrayList<String>();
        studentInfosJSONs.add("{\"name\":\"Leo\",\"age\":18}");
        studentInfosJSONs.add("{\"name\":\"Marry\",\"age\":17}");
        studentInfosJSONs.add("{\"name\":\"Jack\",\"age\":19}");
        //sparksession的相当于sqlSpark,里面是sparkContext不是javaSparkContext,不能序列化集合List
        JavaRDD<String> studentInfosJSONsRDD =sc.parallelize(studentInfosJSONs);

        //json格式的RDD->DF
        Dataset<Row> studentInfoDF = sqlContext.read().json(studentInfosJSONsRDD);

        //针对学生基本信息df,注册临时表，查询
        studentInfoDF.createOrReplaceTempView("student_infos");
        String sql = "select name,age from student_infos where name in (";
        for(int i = 0;i<goodStudentsNames.size();i++){
            sql+="'"+goodStudentsNames.get(i)+"'";
            if(i<goodStudentsNames.size()-1){
                sql+=",";
            }
        }
        sql+=")";
        Dataset<Row> goodStudentsInfoDF = sqlContext.sql(sql);
        //两个DF(name score和name info)->rdd-->map-->JavaPairRDD-->join ===>name,(score,age)
        JavaPairRDD<String, Tuple2<Integer, Integer>> goodStusRDD =
                goodStudentScoresDF.javaRDD().mapToPair(new PairFunction<Row, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Row row) throws Exception {
                return new Tuple2<String, Integer>(row.getString(0),
                        Integer.valueOf(String.valueOf(row.getLong(1))));
            }
        }).join(goodStudentsInfoDF.javaRDD().mapToPair(new PairFunction<Row, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Row row) throws Exception {
                return new Tuple2<String, Integer>(row.getString(0),
                        Integer.valueOf(String.valueOf(row.getLong(1))));
            }
        }));

        //将封装在rdd的好学生的全部信息转为javaRDD<Row>:javaRdd->df
        JavaRDD<Row> goosStusRowsRDD = goodStusRDD.map(new Function<Tuple2<String, Tuple2<Integer, Integer>>, Row>() {
            @Override
            public Row call(Tuple2<String, Tuple2<Integer, Integer>> t) throws Exception {
                return RowFactory.create(t._1, t._2._1, t._2._2);  //name,score,age
            }
        });

        //创建元数据-->DF
        ArrayList<StructField> structFields = new ArrayList<>();
        structFields.add(DataTypes.createStructField("name",DataTypes.StringType,true));
        structFields.add(DataTypes.createStructField("score",DataTypes.IntegerType,true));
        structFields.add(DataTypes.createStructField("age",DataTypes.IntegerType,true));
        StructType structType = DataTypes.createStructType(structFields);

        //rdd-->DF
        Dataset<Row> goodStusDF = sqlContext.createDataFrame(goosStusRowsRDD, structType);
        goodStusDF.write().json("hdfs://spark1:9000/spark-study/good_students");


    }
}
