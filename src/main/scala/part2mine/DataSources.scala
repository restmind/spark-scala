package part2mine

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types.{DateType, DoubleType, LongType, StringType, StructField, StructType}
import part2dataframes.DataSources.spark
import part2mine.DataFrameBasics.spark

object DataSources extends App {

  val spark = SparkSession.builder()
    .appName("DataSources")
    .config("spark.master", "local")
    .getOrCreate()

  val carsSchema = StructType(Array(
    StructField("Name", StringType),
    StructField("Miles_per_Gallon", DoubleType),
    StructField("Cylinders", LongType),
    StructField("Displacement", DoubleType),
    StructField("Horsepower", LongType),
    StructField("Weight_in_lbs", LongType),
    StructField("Acceleration", DoubleType),
    StructField("Year", DateType),
    StructField("Origin", StringType)
  ))

  /*
    Reading a DF:
    - format
    - schema or inferSchema = true
    - path
    - zero or more options
   */
  val carsDF = spark.read
    .format("json")
    .schema(carsSchema) // enforce a schema
    .option("mode", "failFast") // dropMalformed, permissive (default)
    .load("src/main/resources/data/cars.json")

  //CSV
  val stockSchema = StructType(Array(
    StructField("symbol", StringType),
    StructField("date", DateType),
    StructField("price", DoubleType)
  ))

  spark.read
    .schema(stockSchema)
    .option("dateFormat", "MMM dd YYYY")
    .option("header", "true")
    .option("sep", ",")
    .option("nullValue", "")
    .csv("src/main/resources/data/stocks.csv")


  //Parquet
  carsDF.write
    .mode(SaveMode.Overwrite)
    .parquet("src/main/resources/data/cars.parquet")

  //spark.read.text("").show()


  //DB
  val driver = "org.postgresql.Driver"
  val url = "jdbc:postgresql://localhost:5432/rtjvm"
  val user = "docker"
  val password = "docker"

  val employeesDF = spark.read
    .format("jdbc")
    .option("driver", driver)
    .option("url", url)
    .option("user", user)
    .option("password", password)
    .option("dbtable", "public.employees")
    .load()

  employeesDF.show()

  val cinemaSchema = StructType(Array(
    StructField("Title", StringType),
    StructField("US_Gross", LongType),
    StructField("Worldwide_Gross", LongType),
    StructField("US_DVD_Sales", StringType),
    StructField("Production_Budget", LongType),
    StructField("Release_Date", StringType),
    StructField("MPAA_Rating", StringType),
    StructField("Running_Time_min", StringType),
    StructField("Distributor", StringType),
    StructField("Source", StringType),
    StructField("Major_Genre", StringType),
    StructField("Creative_Type", StringType),
    StructField("Director", StringType),
    StructField("Rotten_Tomatoes_Rating", DoubleType),
    StructField("IMDB_Rating", DoubleType),
    StructField("IMDB_Votes", LongType)
  ))

  val cinemaDF = spark.read
    .schema(cinemaSchema)
    .json("src/main/resources/data/movies.json")

  cinemaDF.write
    .mode(SaveMode.Overwrite)
    .option("header", "true")
    .option("sep", "\t")
    .csv("src/main/resources/data/cinema-copy.csv")

  cinemaDF.write.save("src/main/resources/data/cinema-copy.parquet")

  cinemaDF.write
    .format("jdbc")
    .option("driver", driver)
    .option("url", url)
    .option("user", user)
    .option("password", password)
    .option("dbtable", "public.employees")
    .save()


  spark.sql(
    """
      |
      |""".stripMargin)
}
