package ml.upcedu

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}


/**
 * @description Poetry Number in Tang & Song
 * @author by zhoutao
 * @date 2021/6/10 10:54
 */
object PoetrySum {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hadoop")
    System.setProperty("user.name", "hadoop")

    val spark: SparkSession = SparkSession
      .builder()
      .appName("PoetrySum")
//      .master("local[*]")
      .getOrCreate()

    val sc: SparkContext = spark.sparkContext
    sc.setLogLevel("WARN")
    import spark.implicits._

    val tang_count: Long = sumPoetry(spark, "hdfs://Master:9000/user/hadoop/poetry/poet.tang*")
    println("唐朝一共有 " + tang_count + " 首古诗")

    val song_count: Long = sumPoetry(spark, "hdfs://Master:9000/user/hadoop/poetry/poet.song*")
    println("宋朝一共有 " + song_count + " 首古诗")

    val total: Long = tang_count + song_count
    println("一共有 " + total + " 首古诗")
  }

  def sumPoetry(spark: SparkSession, path: String): Long ={
    val df: DataFrame = spark.read.option("multiline", "true").json(path)
    val rdd: RDD[Row] = df.select("paragraphs").rdd
    val count = rdd.distinct(1).count()
    count
  }
}
