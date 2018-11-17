#spark2.x
export SPARK_HOME=/usr/local/spark-2.3.2
export CLASSPATH=$($HADOOP_HOME/bin/hadoop classpath):/usr/local/spark-2.3.2/jars/*:$CLASSPATH
/usr/local/spark/bin/spark-submit --master yarn-cluster --num-executors 10 --executor-memory 5G --conf spark.yarn.executor.memoryOverhead=2048 --driver-memory 5G --class UserVisitSessionAnalyzeSpark23 spark23/target/spark23-1.0-SNAPSHOT.jar



#spark1.x
export SPARK_HOME=/usr/local/spark
export CLASSPATH=$($HADOOP_HOME/bin/hadoop classpath):/usr/local/spark/lib/*:$CLASSPATH
/usr/local/spark/bin/spark-submit --master yarn-cluster --num-executors 10 --executor-memory 5G --conf spark.yarn.executor.memoryOverhead=2048 --driver-memory 5G --class UserVisitSessionAnalyzeSpark16 spark16/target/spark16-1.0-SNAPSHOT.jar


