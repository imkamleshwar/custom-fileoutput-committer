package fileOutputCommittter

import customFileCommitter.OBFileOutputCommitter
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.sql.SparkSession

object TryingOut extends App {

  val spark = SparkSession
    .builder()
    .config("spark.sql.sources.outputCommitterClass", "customFileCommitter.OBFileOutputCommitter")
    .master("local[2]")
    .appName("File Output Committer")
    .getOrCreate()

  spark.sparkContext.setLogLevel("INFO")

  val jobConf: JobConf = new JobConf(spark.sparkContext.hadoopConfiguration)
  jobConf.setOutputCommitter(classOf[OBFileOutputCommitter])

  spark.conf.getAll.foreach(println)

  val df = spark.read.option("header", "true").csv("src/main/resources/tripFiles/*.csv")

  df.show(false)

  df.coalesce(1).write.orc("target/tripFiles/test01/")

}
