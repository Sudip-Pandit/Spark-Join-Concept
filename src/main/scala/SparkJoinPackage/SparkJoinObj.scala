package SparkJoinPackage
import org.apache.spark._

import sys.process._
import org.apache.spark.sql.{Row, SparkSession, types}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.SparkContext.jarOfObject
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
object SparkJoinObj {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("ComplexDataProcessing").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val spark = SparkSession.builder()
      .config("fs.s3a.access.key", "")
      .config("fs.s3a.secret.key", "")
      .getOrCreate()

    val df1 = spark.read.format("csv").option("header", "true")
      .load("file:///D:/data1/file.txt")
    df1.show()
    df1.printSchema()

    val df2 = spark.read.format("csv").option("header","true")
      .load("file:///D:/data1/file2.txt")

    df2.show()
    df2.printSchema()

    val joindf = df1.join(df2, df1("id")===df2("custid"), "inner")
      .drop(df2("custid"))
    joindf.show()
    joindf.printSchema()


  }
}