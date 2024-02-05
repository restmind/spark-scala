package part2mine

import org.apache.spark.sql.SparkSession

object RDDs extends App {
  val spark = SparkSession.builder()
    .appName("RDDs")
    .config("spark.master", "local")
    .getOrCreate()

  val sc = spark.sparkContext
}
