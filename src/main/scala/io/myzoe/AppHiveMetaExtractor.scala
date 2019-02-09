package io.myzoe

import java.io.File

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.aggregate._


object AppHiveMetaExtractor {
  println( "Extracting Hive Meta Table and Column structures" )

  val spark: SparkSession =
    SparkSession
      .builder()
      .appName("Extracting Hive Meta Table and Column structures")
      .config("spark.master", "local")
      .enableHiveSupport()
      .getOrCreate()

  //spark.sparkContext.setLogLevel("ERROR")

  import spark.implicits._
  import org.apache.spark.sql.functions._

  def main(args: Array[String]): Unit = {

    if (args.isEmpty) {
      println("Targetlocation parameter is missing for exporting metadata")
      System.exit(1)
    }

    val targetLocation = args(0)

    spark.sql("use default")
    //spark.sql("CREATE DATABASE IF NOT EXISTS landing")
    //spark.sql("CREATE TABLE IF NOT EXISTS landing.customer (customer_id INT, customer_name STRING) USING hive")
    //spark.sql("CREATE TABLE IF NOT EXISTS landing.product (product_id INT, product_name STRING, product_desc STRING, product_cd VARCHAR(50)) USING hive")
    //spark.sql("CREATE TABLE IF NOT EXISTS landing.organization (organization_id INT, organization_name STRING, organization_desc STRING, organization_cd VARCHAR(50)) USING hive")
    //spark.sql("CREATE TABLE IF NOT EXISTS landing.transaction (transaction_id INT, transaction_name STRING, transaction_desc STRING, transaction_cd VARCHAR(50)) PARTITIONED BY (reporting_dt timestamp)")

    spark.sql("DROP TABLE IF EXISTS all_tables")
    spark.sql("CREATE TABLE IF NOT EXISTS all_tables (owner STRING, table_name STRING, table_type STRING, table_provider STRING, table_properties STRING, table_location STRING, table_serde_properties STRING, table_storage_properties STRING) ")
    spark.sql("TRUNCATE TABLE all_tables")

    spark.sql("DROP TABLE IF EXISTS all_table_columns")
    spark.sql("CREATE TABLE IF NOT EXISTS all_table_columns (owner STRING, table_name STRING, column_name STRING, column_order_id INT, data_type STRING, comment STRING, partition_column_ind VARCHAR(1)) ")
    spark.sql("TRUNCATE TABLE all_table_columns")

    val databaseList = spark.sql("show databases")

    //databaseList.show()

    databaseList.collect().map({
      db => {
        val currentDB = db.getString(0)
        spark.sql(s"use ${currentDB}")

        val tableList = spark.sql("show tables")

        tableList.collect().map{
          tb => {
            val own = tb.getString(0)
            if (!own.isEmpty) {

              val tbl = tb.getString(1)
              val columnListExtra = spark.sql(s"describe formatted ${tbl}")

              // Table details

              //println(own)
              //println(tbl)
              //columnListExtra.show()


              val tblType = columnListExtra.filter($"col_name" === "Type").select($"data_type").head().getString(0)
              val tblProvider = columnListExtra.filter($"col_name" === "Provider").select($"data_type").head().getString(0)
              val tblProperties = columnListExtra.filter($"col_name" === "Table Properties").select($"data_type").head().getString(0)
              val tblLocation = columnListExtra.filter($"col_name" === "Location").select($"data_type").head().getString(0)
              val tblSerdeProperties = columnListExtra.filter($"col_name" === "Serde Library").select($"data_type").head().getString(0)
              val tblStorageProperties = columnListExtra.filter($"col_name" === "Storage Properties").select($"data_type").head().getString(0)

              spark.sql(s"insert into default.all_tables select '${own}' as owner, '${tbl}' as table_name, '${tblType}' as table_type, '${tblProvider}' as table_provider, '${tblProperties}' as table_properties, '${tblLocation}' as table_location, '${tblSerdeProperties}' as table_serde_properties, '${tblStorageProperties}' as table_storage_properties")

              // Column details
              val columnList = spark.sql(s"show columns in ${tbl}").select($"col_name".as("column_name"))
                .withColumn("column_order_id", monotonically_increasing_id() + 1)

              val columnListJoined = columnListExtra.join(columnList, columnListExtra("col_name") === columnList("column_name"), "left")

              val realCols = columnListJoined.filter($"column_name" isNotNull).select($"col_name", $"data_type", $"comment", $"column_order_id")
                .groupBy("col_name", "data_type", "comment", "column_order_id").agg(count("col_name").as("num_of_col"))
                .withColumn("partition_column_ind", when($"num_of_col" === 1, "N").otherwise("Y"))
                .withColumn("table_name", lit(tbl))
                .withColumn("owner", lit(own))
                .createOrReplaceTempView("tmp_load_all_table_columns")

              spark.sql("insert into default.all_table_columns select owner, table_name, col_name, column_order_id, data_type, comment, partition_column_ind from tmp_load_all_table_columns order by owner, table_name, column_order_id")
            }
          }
        }

      }
    })


    spark.sql("select * from default.all_tables").repartition(1).write.option("delimiter", ";").option("header", "true").csv(targetLocation + "/metadata/all_tables")
    spark.sql("select * from default.all_table_columns").repartition(1).write.option("delimiter", ";").option("header", "true").csv(targetLocation + "/metadata/all_table_columns")
  }

}
