package fileOutputCommittter

import org.apache.spark.sql.{SaveMode, SparkSession}

object TryingOut1 extends App {

  val spark = SparkSession
    .builder()
    .config("spark.sql.sources.commitProtocolClass", "commitProtocol.CustomSQLHadoopMapReduceCommitProtocol")
    .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
    .master("local[2]")
    .appName("File Output Committer")
    .getOrCreate()

  spark.sparkContext.setLogLevel("INFO")

  spark.conf.getAll.foreach(println)

  val df = spark.read.option("header", "true").csv("src/main/resources/tripFiles/*.csv")

  df.show(false)

  df.coalesce(1).write.mode(SaveMode.Overwrite).orc("target/tripFiles/test01/")

}
