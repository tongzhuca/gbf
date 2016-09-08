
import java.io.File

import com.typesafe.config.ConfigFactory
import org.apache.commons.io.FileUtils
import org.apache.spark._
import org.apache.spark.sql.{DataFrame, Row, SQLContext}

object BinExpandApp
  extends App {
  val config = ConfigFactory.load("hnl.conf")
  val appConf = ConfigFactory.load()
  val dataPath = config.getString(args(0))
  //  println("dataFile:" + dataPath)

  val master = "local"
  val appName = "BinExpandApp"
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

  def appendToFile(r: Row) = {
    // TODO: ts1 and ts2 can start with 0
    // a quick fix is to ignore this record
    var ts1 = r.getDouble(1).toInt.toString
    var ts2 = r.getDouble(2).toInt.toString
    ts1.length match {
      case 4 => ts1 = ts1 + "00"
        ts2 = ts2 + "00"
      case 5 => ts1 = ts1 + "0"
        ts2 = ts2 + "0"
      case _ if ts1.length > 6 => ts1 = ts1.substring(0, 6)
        ts2 = ts2.substring(0, 6)
      case _ => "wrong one: " + println(r)
    }

    val r1 = ts1.toInt
    val r2 = ts2.toInt

    var a = 0L
    for (a <- r1 to r2) {
      val all = a :: r.toSeq.toList.drop(3)
      val ss = all.mkString(",") + "\n"

      FileUtils.write(binResultFile, ss, true)
    }
  }

  dfOriginalBinInfo.foreach(r => appendToFile(r))
}
