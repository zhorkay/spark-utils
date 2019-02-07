package io.myzoe

import java.nio.file.Paths

import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window


object App {
  println( "Hello World!" )

  val spark: SparkSession =
    SparkSession
      .builder()
      .appName("Moving Average with window function")
      .config("spark.master", "local")
      .getOrCreate()


  import spark.implicits._
  import org.apache.spark.sql.functions._

  def main(args: Array[String]): Unit = {
    val csvSchema = StructType(Array(
      StructField("YearMonth", DateType, true),
      StructField("FUNC_ID", IntegerType, true),
      StructField("COUNTRY_CD", IntegerType, true),
      StructField("LOCAL_AMT", DoubleType, true)))

    val df = spark.read
      .option("header", "true")
      .option("delimiter", ";")
      .schema(csvSchema)
      .csv(fsPath("/t21_series.csv"))

    println(df.columns.size)
    df.printSchema()
    println(df.columns.size)
    df.show()
    df.createGlobalTempView("t21")
//"sum(LOCAL_AMT) over(partition by FUNC_ID, COUNTRY_CD order by YearMonth rows BETWEEN 4 PRECEDING AND CURRENT ROW) as t21_amt_row, " +
    val finalDf = spark.sql(
      "select " +
        "YearMonth, " +
        "FUNC_ID, " +
        "COUNTRY_CD, " +
        "LOCAL_AMT, " +
        "sum(LOCAL_AMT) over(partition by FUNC_ID, COUNTRY_CD order by YearMonth rows BETWEEN 4 PRECEDING AND CURRENT ROW) as t21_amt_range " +
        "from global_temp.t21 order by 2, 3, 1"
    )

    val t21WindowRows = Window.partitionBy($"FUNC_ID", $"COUNTRY_CD").orderBy($"YearMonth").rowsBetween( -4, Window.currentRow)
    val t21WindowRange = Window.partitionBy($"FUNC_ID", $"COUNTRY_CD").orderBy($"YearMonth").rangeBetween(-4, Window.currentRow)

    val finalDf2 = df.select(
      $"*", sum($"LOCAL_AMT").over(t21WindowRows).alias("t21_rows_amt")
    ).orderBy($"FUNC_ID", $"COUNTRY_CD", $"YearMonth")

    finalDf2.show(100)

    spark.close()
  }

  def fsPath(resource: String): String =
    Paths.get(getClass.getResource(resource).toURI).toString
}
