package ml.upcedu

import com.hankcs.hanlp.HanLP
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}


/**
 * @description Poetry Num in Tang & Song
 * @author by zhoutao
 * @date 2021/6/10 11:15
 */
object PoetRank {
  def main(args: Array[String]): Unit = {
    var topNum = 0
    if (args.length <= 0){
      topNum = 20
    } else{
      topNum = args(0).toInt
    }
    System.setProperty("HADOOP_USER_NAME", "hadoop")
    System.setProperty("user.name", "hadoop")

    val spark: SparkSession = SparkSession
      .builder()
      .appName("PoetryRank")
//      .master("local[*]")
      .getOrCreate()

    val sc: SparkContext = spark.sparkContext
    sc.setLogLevel("WARN")
    import spark.implicits._

    val tdf: DataFrame = spark.read.option("multiline", "true").json("hdfs://Master:9000/user/hadoop/poetry/poet.tang*")
    val trdd: RDD[Row] = tdf.select("author", "paragraphs").rdd
    println("===== Top " + topNum + " Poets by poetry number in Tang Empire=====")
    computation(trdd, topNum)

    val sdf: DataFrame = spark.read.option("multiline", "true").json("hdfs://Master:9000/user/hadoop/poetry/poet.song*")
    val srdd: RDD[Row] = sdf.select("author", "paragraphs").rdd
    println("===== Top " + topNum + " Poets by poetry number in Song Empire =====")
    computation(srdd, topNum)

    val ardd: RDD[Row] = trdd.union(srdd)
    println("===== Top " + topNum + " Poets by poetry number in Tang & Song Empire =====")
    computation(ardd, topNum)
  }


  def computation(rdd: RDD[Row], topNum: Int): Unit ={
    rdd.filter(line => line.getList(1).size() > 1)
      .map(line => (line.get(0), 1))
      .reduceByKey(_+_)
      .map(_.swap)
      .sortByKey(ascending = false)
      .map(_.swap)
      .take(topNum)
      .foreach(result => println("Poet: "
        + HanLP.convertToSimplifiedChinese(result._1.toString)
        + ", Poetry Num: " + result._2)
      )
  }
}
