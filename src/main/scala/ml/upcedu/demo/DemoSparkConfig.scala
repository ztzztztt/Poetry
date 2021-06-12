package ml.upcedu.demo

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @description WordCount
 * @author by zhoutao
 * @date 2021/6/9 14:57
 */
object DemoSparkConfig {

  case class JsonClass(strains: List[String],
                       author: String,
                       paragraphs: List[String],
                       title: String)

  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hadoop")
    System.setProperty("user.name", "hadoop")

    val conf: SparkConf = new SparkConf()
      .setAppName("DemoSparkConfig")
//      .setMaster("local[*]")
//      .setMaster("spark://172.19.138.9:7077")
//      .set("spark.driver.host", "180.201.176.81")
//      .setJars(List("E:\\IDEA\\Poetry\\target\\Poetry.jar"))
    val sc: SparkContext = new SparkContext(conf)
    sc.setLogLevel("WARN")

    val testRdd: RDD[String] = sc.textFile(
      "hdfs://172.19.138.9:9000/user/hadoop/poetry/poet.tang.0.json"
    )
    testRdd.foreach(println)
    sc.stop()
  }
}
