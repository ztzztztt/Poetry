package ml.upcedu

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
 * @description Poet Number in Tang & Song
 * @author by zhoutao
 * @date 2021/6/10 10:22
 */
object PoetSum {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hadoop")
    System.setProperty("user.name", "hadoop")

    val spark: SparkSession = SparkSession
      .builder()
      .appName("PoetSum")
//      .master("local[*]")
      .getOrCreate()
    val sc: SparkContext = spark.sparkContext
    sc.setLogLevel("WARN")
    import spark.implicits._

    val tang_count: Long = sumPoet(spark, "hdfs://172.19.138.9:9000/user/hadoop/poetry/authors.tang*")
    println("唐朝一共有 " + tang_count + " 诗人")

    val song_count: Long = sumPoet(spark, "hdfs://172.19.138.9:9000/user/hadoop/poetry/authors.song*")
    println("宋朝一共有 " + song_count + " 诗人")

    val total: Long = tang_count + song_count
    println("一共有 " + total + " 诗人生活在唐宋两个朝代")
    sc.stop()
  }

  def sumPoet(spark: SparkSession, path: String): Long ={
    import spark.implicits._
    val df: DataFrame = spark.read.option("multiline", "true").json(path)
    val rdd: RDD[Row] = df.select("name").rdd
    val count = rdd.distinct(1).count()
    count
  }
}
