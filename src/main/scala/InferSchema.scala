
import java.io.File

import com.typesafe.config.ConfigFactory
import org.apache.commons.io.FileUtils
import org.apache.spark._
import org.apache.spark.sql.{DataFrame, SQLContext}

object InferSchema
  extends App {
  val config = ConfigFactory.load("hnl.conf")
  val appConf = ConfigFactory.load()
  val dataPath = config.getString(args(0))
  println("dataFile:" + dataPath)

  val master = "local"
  val appName = "SimpleApp"
  val DELIMITER = ","
  val conf = new SparkConf().setMaster(master).setAppName(appName)
    .set("spark.executor.memory", "4g")
    .set("spark.logConf", "true")

  val sparkContext = new SparkContext(conf)
  val sqlContext = new SQLContext(sparkContext)

  val binResultFile = new File("/tmp/result.csv")
  if (binResultFile.exists())
    FileUtils.forceDelete(binResultFile)

  val dfOriginalBinInfo: DataFrame = sqlContext.read
    .format("com.databricks.spark.csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .option("delimiter", DELIMITER)
    .load(dataPath)

  dfOriginalBinInfo.printSchema()

  println("dfOriginalBinInfo:" + dfOriginalBinInfo.count())
}
