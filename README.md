first spark application word count
base on spark2.2.0
you can run it in local environment and submit the jar with all dependencies to the spark cluster
as for running on the spark cluster:you need upload your file to hdfs and write a script of spark-submit:
the spark-submit script are following:

/usr/local/spark/bin/spark-submit \ //使用的工具
--class cn.spark.core.WordCountCluster \ # 执行的主类入口
--num-executors 3 \
--driver-memory 100m \
--executor-memory 100m \
--executor-cores 3 \
/usr/local/spark-study/java/spark-study-java-1.0-SNAPSHOT-jar-with-dependencies.jar \  # jar