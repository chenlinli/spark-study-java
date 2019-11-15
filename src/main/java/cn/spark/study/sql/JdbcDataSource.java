package cn.spark.study.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class JdbcDataSource {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("JdbcDataSource");
        JavaSparkContext sc = new JavaSparkContext(conf);

        SQLContext sqlContext = new SQLContext(sc);
        //分别将mysql两个表--》DF
        HashMap<String, String> options = new HashMap<>();
        options.put("url", "jdbc:mysql://spark1:3306/testdb");
        options.put("driver","com.mysql.jdbc.Driver");
        options.put("dbtable","student_infos");
        Dataset<Row> studentInfoDF = sqlContext.read().format("jdbc")
                .options(options).load();

        options.put("dbtable","student_scores");
        Dataset<Row> studentScoreDF = sqlContext.read().format("jdbc").options(options).load();

        //DF->pairRdd,join
        JavaPairRDD<String, Tuple2<Integer, Integer>> good_student_infosDF =
                studentInfoDF.javaRDD().mapToPair(new PairFunction<Row, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Row row) throws Exception {
                return new Tuple2<>(row.getString(0),
                        Integer.valueOf(String.valueOf(row.get(1))));
            }
        }).join(studentScoreDF.javaRDD().mapToPair(new PairFunction<Row, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Row row) throws Exception {
                return new Tuple2<>(row.getString(0),
                        Integer.valueOf(String.valueOf(row.get(1))));
            }
        }));

        //javaPairRdd==>javaRDD<Row>
        JavaRDD<Row> studentsRowRDD = good_student_infosDF.map(new Function<Tuple2<String, Tuple2<Integer, Integer>>, Row>() {
            @Override
            public Row call(Tuple2<String, Tuple2<Integer, Integer>> t) throws Exception {
                return RowFactory.create(t._1, t._2._1, t._2._2);
            }
        });

        ArrayList<StructField> structFields = new ArrayList<>();
        structFields.add(DataTypes.createStructField("name",DataTypes.StringType,true));
        structFields.add(DataTypes.createStructField("age",DataTypes.IntegerType,true));
        structFields.add(DataTypes.createStructField("score",DataTypes.IntegerType,true));
        StructType structType = DataTypes.createStructType(structFields);

        Dataset<Row> studentDF = sqlContext.createDataFrame(studentsRowRDD, structType);

        List<Row> collect1 = studentDF.javaRDD().collect();
        for(Row r:collect1){
            System.out.println(r);
        }
        //数据保存到jdbc
        studentDF.javaRDD().foreach(new VoidFunction<Row>() {
            @Override
            public void call(Row row) throws Exception {
                String sql = "insert into good_student_infos values("
                        +"'"+String.valueOf(row.get(0))+"' ,"+
                        Integer.valueOf(String.valueOf(row.get(1)))+","+
                        Integer.valueOf(String.valueOf(row.get(2)))+")";

                Class.forName("com.mysql.jdbc.Driver");
                Connection con = null;
                Statement stmt = null;
                try {
                    con = DriverManager.getConnection("jdbc:mysql://spark1:3306/testdb",
                            "", "");
                    stmt = con.createStatement();
                    stmt.executeUpdate(sql);

                }catch (Exception e){
                    e.printStackTrace();
                }finally {
                    if(stmt!=null){
                        stmt.close();
                    }
                    if(con!=null) {
                        con.close();
                    }
                }
            }
        });


    }
}
