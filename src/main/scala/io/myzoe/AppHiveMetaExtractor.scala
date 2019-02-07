package io.myzoe

import java.io.File

import org.apache.spark.sql.SparkSession

object AppHiveMetaExtractor {
  println( "Extracting Hive Meta Table and Column structures" )

  val warehouseLocation = new File("spark-warehouse").getAbsolutePath

  val spark: SparkSession =
    SparkSession
      .builder()
      .appName("Extracting Hive Meta Table and Column structures")
      //.config("spark.sql.warehouse.dir", warehouseLocation)
      .config("spark.master", "local")
      .enableHiveSupport()
      .getOrCreate()


  import spark.implicits._
  import org.apache.spark.sql.functions._

  def main(args: Array[String]): Unit = {

    spark.sql("CREATE DATABASE IF NOT EXISTS landing")
    spark.sql("CREATE TABLE IF NOT EXISTS landing.customer (customer_id INT, customer_name STRING) USING hive")
    spark.sql("CREATE TABLE IF NOT EXISTS landing.product (product_id INT, product_name STRING, product_desc STRING, product_cd VARCHAR(50)) USING hive")
    spark.sql("CREATE TABLE IF NOT EXISTS landing.organization (organization_id INT, organization_name STRING, organization_desc STRING, organization_cd VARCHAR(50)) USING hive")
    spark.sql("CREATE TABLE IF NOT EXISTS landing.transaction (transaction_id INT, transaction_name STRING, transaction_desc STRING, transaction_cd VARCHAR(50)) PARTITIONED BY (reporting_dt timestamp)")
    spark.sql("use landing")

    val tableList = spark.sql("show tables")

    tableList.show()

    tableList.collect().map{
      r => {
        val columnList = spark.sql(s"describe formatted ${r.getString(1)}")
        columnList.show()
        r
      }
    }

  }
}
