package part2mine

import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.types.{DateType, DoubleType, LongType, StringType, StructField, StructType}
import org.apache.spark.sql.functions.{avg, col, column, countDistinct, count_distinct, expr, mean, min, stddev}


object ColumnsAndExpressions extends App {
  val spark = SparkSession.builder()
    .appName("Columns and Expressions")
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

  val carsDF = spark.read
    .schema(carsSchema) // enforce a schema
    .option("mode", "failFast") // dropMalformed, permissive (default)
    .json("src/main/resources/data/cars.json")

  val firstColumn =carsDF.col("Name")


  val carNamesDF = carsDF.select(firstColumn)
  carNamesDF.show()

  import spark.implicits._


  val weightInKgExpression = carsDF.col("Weight_in_lbs") / 2.2

  val carWithWeightDF = carsDF.select(
    col("Name"),
    col("Weight_in_lbs"),
    weightInKgExpression.as("Weight_in_kg")
  )

  carWithWeightDF.show()

  val carsWithSelectExprWeightsDF = carsDF.selectExpr(
    "Name",
    "Weight_in_lbs",
    "Weight_in_lbs / 2.2"
  )


  val carsWithKg3DF = carsDF.withColumn("Weight_in_kg_3", col("Weight_in_lbs") / 2.2)


  val notAmerica = carsDF.filter(col("Origin") =!= "USA")
  val notAmerica2 = carsDF.where(col("Origin") =!= "USA")
  val americanCarsDF = carsDF.filter("Origin = 'USA' and Horsepower > 150")

/*  val moreCarsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/more_cars.json")

  val allCars = carsDF.union(moreCarsDF)*/










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


  val mvDF = cinemaDF.select(
    col("Title"),
    col("Release_Date")
  )
  mvDF.show()

  val extraColumn = cinemaDF.withColumn("Total_profit", col("US_Gross") + col("Worldwide_Gross") + col("US_DVD_Sales"))

  val comedies = cinemaDF.where(col("Major_Genre")==="Comedy" and col("IMDB_Rating") > 6.0)

  comedies.show()

  cinemaDF.select(countDistinct("Major_Genre"))
  cinemaDF.selectExpr("count(Major_Genre)")

  cinemaDF.select(min(col("IMDB_Rating")))
  cinemaDF.selectExpr("min(IMDB_Rating)")


  cinemaDF.selectExpr(("(US_Gross + Worldwide_Gross + US_DVD_Sales) as Total_Gross"))
    .select(functions.sum("Total_Gross"))
    .show()

  cinemaDF.select(countDistinct("Director")).show()


  cinemaDF.select(
    mean(col("US_Gross")),
    stddev(col("US_Gross"))
  ).show()

  cinemaDF
    .groupBy(col("Director"))
    .agg(
      avg(col("IMDB_Rating")).as("AVG_Rating"),
      avg(col("US_Gross")).as("AVG_REVENUE")
    )


}
