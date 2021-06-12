package ml.upcedu

import com.hankcs.hanlp.HanLP
import com.hankcs.hanlp.tokenizer.StandardTokenizer
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
 * @description Frequent words
 * @author by zhoutao
 * @date 2021/6/10 21:28
 */
object FrequentWord {
  def main(args: Array[String]): Unit = {
    var topNum = 0
    var empire = "tang"
    if (args.length <= 0){
      empire = "tang"
      topNum = 10
    } else if(args.length == 1){
      empire = args(0)
    } else{
      empire = args(0)
      topNum = args(1).toInt
    }

    System.setProperty("HADOOP_USER_NAME", "hadoop")
    System.setProperty("user.name", "hadoop")

    val spark: SparkSession = SparkSession
      .builder()
      .appName("FrequentWord")
//      .master("local[*]")
      .getOrCreate()

    val sc: SparkContext = spark.sparkContext
    sc.setLogLevel("WARN")
    import spark.implicits._

    var path = ""
    if (empire == "all"){
      path = "hdfs://Master:9000/user/hadoop/poetry/poet*"
    } else{
      path = "hdfs://Master:9000/user/hadoop/poetry/poet." + empire + "*"
    }
    println("Load data from '" + path + "'")
    val tdf: DataFrame = spark.read.option("multiline", "true").json(path)
    val trdd: RDD[Row] = tdf.select("paragraphs").rdd
    // 将 List[List[String]] 转换成 String
    val prdd: RDD[String] = trdd.map(row => row(0).asInstanceOf[Seq[String]].toList).flatMap(list => list)

    // 处理诗，将古诗进行分词, 并进行统计
    val frdd: RDD[(Int, String)] = prdd
      .flatMap(
        line => {
          // 将中文繁体转化成简体，转换后将中文进行分词
          StandardTokenizer.segment(HanLP.convertToSimplifiedChinese(line)).toArray
        })
      // 去除中文分词产生的 ，。/n等符号
      .map(word => word.toString.replaceAll("[\\[\\]，。/a-z]", ""))
      // 去除分词的单个字以及空字符串
      .filter(word => word != "" && word.length > 1)
      // 计算统计频率
      .map(word => (word, 1))
      .reduceByKey(_+_)
      .map(_.swap)
      .sortByKey(ascending = false)
    // 输出结果
    frdd.take(topNum).foreach(println)
    // 关闭context配置文件
    sc.stop()
  }
}
