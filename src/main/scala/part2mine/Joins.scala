package part2mine

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, max}
import part2mine.DataSources.spark

object Joins extends App {
  val spark = SparkSession.builder()
    .appName("Joins")
    .config("spark.master", "local")
    .getOrCreate()


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

  val salaryDF = spark.read
    .format("jdbc")
    .option("driver", driver)
    .option("url", url)
    .option("user", user)
    .option("password", password)
    .option("dbtable", "public.salaries")
    .load()

  val managerDF = spark.read
    .format("jdbc")
    .option("driver", driver)
    .option("url", url)
    .option("user", user)
    .option("password", password)
    .option("dbtable", "public.dept_manager")
    .load()

  val maxSalaryDF = salaryDF.groupBy(col("emp_no")).agg(max(col("salary")))
  employeesDF.join(maxSalaryDF, employeesDF.col("emp_no") === salaryDF.col("emp_no"), "outer")


  employeesDF.join(managerDF, employeesDF.col("emp_no") === managerDF.col("emp_no"), "left_anti")
  /**
   * Exercises
   *
   * 1. show all employees and their max salary
   * 2. show all employees who were never managers
   * 3. find the job titles of the best paid 10 employees in the company
   */
}
