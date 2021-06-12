package ml.upcedu.demo

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * @description TODO
 * @author by zhoutao
 * @date 2021/6/10 9:44
 */
object DemoSparkSession {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hadoop")
    System.setProperty("user.name", "hadoop")

    val spark: SparkSession = SparkSession
      .builder()
      .appName("DemoSparkSQL")
//      .master("local[*]")
      .getOrCreate()

    val sc: SparkContext = spark.sparkContext
    sc.setLogLevel("WARN")

    val df: DataFrame = spark.read.option("multiline", "true").json("hdfs://Master:9000/user/hadoop/poetry/authors*")
    val rdd = df.select("name").rdd
    rdd.map(line => (line, 1))
      .reduceByKey(_+_)
      .map(_.swap)
      .sortByKey(ascending = false)
      .foreach(println)
    sc.stop()
  }
}
