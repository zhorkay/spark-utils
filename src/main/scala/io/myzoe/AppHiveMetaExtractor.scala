package io.myzoe

import java.io.File

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.aggregate._


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

  spark.sparkContext.setLogLevel("ERROR")


  import spark.implicits._
  import org.apache.spark.sql.functions._

  def main(args: Array[String]): Unit = {

    spark.sql("CREATE DATABASE IF NOT EXISTS landing")
    spark.sql("CREATE TABLE IF NOT EXISTS landing.customer (customer_id INT, customer_name STRING) USING hive")
    spark.sql("CREATE TABLE IF NOT EXISTS landing.product (product_id INT, product_name STRING, product_desc STRING, product_cd VARCHAR(50)) USING hive")
    spark.sql("CREATE TABLE IF NOT EXISTS landing.organization (organization_id INT, organization_name STRING, organization_desc STRING, organization_cd VARCHAR(50)) USING hive")
    spark.sql("CREATE TABLE IF NOT EXISTS landing.transaction (transaction_id INT, transaction_name STRING, transaction_desc STRING, transaction_cd VARCHAR(50)) PARTITIONED BY (reporting_dt timestamp)")
    spark.sql("CREATE TABLE IF NOT EXISTS landing.all_tables (owner string, table_name STRING) ")
    spark.sql("use landing")

    val tableList = spark.sql("show tables")

    tableList.select($"database".as("owner"), $"tableName".as("table_name")).createOrReplaceTempView("tab_meta")
    spark.sql("TRUNCATE TABLE landing.all_tables")
    spark.sql("insert into landing.all_tables select * from tab_meta")
    spark.sql("select * from landing.all_tables").show()



    tableList.collect().map{
      r => {
        val own = r.getString(0)
        val tbl = r.getString(1)
        val columnListExtra = spark.sql(s"describe formatted ${tbl}")
        val columnList = spark.sql(s"show columns in ${r.getString(1)}").select($"col_name".as("column_name"))
          .withColumn("column_order_id", monotonically_increasing_id()+1)
        val finalCols = columnListExtra.join(columnList, columnListExtra("col_name") === columnList("column_name"),"left")
        val tblType = columnListExtra.filter($"col_name" === "Type").select($"data_type").head().getString(0)
        val tblProvider = columnListExtra.filter($"col_name" === "Provider").select($"data_type").head().getString(0)
        val tblProperties = columnListExtra.filter($"col_name" === "Table Properties").select($"data_type").head().getString(0)
        val tblLocation = columnListExtra.filter($"col_name" === "Location").select($"data_type").head().getString(0)

        val realCols = finalCols.filter($"column_name" isNotNull).select($"col_name", $"data_type", $"comment", $"column_order_id")
          .groupBy("col_name", "data_type", "comment", "column_order_id").agg(count("col_name").as("num_of_col"))
          .withColumn("partition_column_ind", when($"num_of_col" === 1, "N").otherwise("Y"))
          .withColumn("table_name", lit(tbl))
          .withColumn("owner", lit(own))
          .orderBy($"column_order_id")
        //finalCols.show()
        realCols.show()
        spark.sql(s"select '${own}' as owner, '${tbl}' as table_name, '${tblType}' as table_type, '${tblProvider}' as table_provider, '${tblProperties}' as table_properties, '${tblLocation}' as table_location" ).show()
        r
      }
    }

  }
}
