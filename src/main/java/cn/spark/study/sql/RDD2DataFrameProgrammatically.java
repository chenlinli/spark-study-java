package cn.spark.study.sql;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

//编程方式
public class RDD2DataFrameProgrammatically {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("RDD2DataFrameProgrammatically")
                .master("local").getOrCreate();
        //创建普通rdd，元素类型是Row：RDD<Row>
        //报错：java.lang.String is not a valid external type for schema of int
        //有数据定义为String，转成int
        //所以在sql里，age<18的语法，强行要将age转换为Integer,但是之前肯定义为String
        //row里放数据的时候，什么格式的是转换下,不直接放String
        JavaRDD<Row> rows = spark.read().textFile("src/students.txt")
                .toJavaRDD().map(new Function<String, Row>() {
                    @Override
                    public Row call(String s) throws Exception {
                        String[] split = s.split(",");
                        return RowFactory.create(Integer.parseInt(split[0]),
                                split[1], Integer.parseInt(split[2]));
                    }
                });

        //动态构造元素据
        //id,name等类型，可能都是运行过程加载的，编写的时候是不确定的，所有编程时构造元数据很合适
        ArrayList<StructField> structFields = new ArrayList<>();
        structFields.add(DataTypes.createStructField("id",DataTypes.IntegerType,true));
        structFields.add(DataTypes.createStructField("name",DataTypes.StringType,true));
        structFields.add(DataTypes.createStructField("age",DataTypes.IntegerType,true));

        StructType structType = DataTypes.createStructType(structFields);

        //使用动态构造的元数据将RDD转换为DataFrame
        Dataset<Row> studentDF = spark.createDataFrame(rows, structType);
        
        //使用df
        studentDF.createOrReplaceTempView("students");
        Dataset<Row> teenagerDF = spark.sql("select * from students where age < 18");
        List<Row> list = teenagerDF.javaRDD().collect();

        for(Row r:list){
            System.out.println(r);
        }
    }
}
