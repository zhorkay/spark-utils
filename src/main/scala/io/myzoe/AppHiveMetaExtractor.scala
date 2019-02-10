package io.myzoe

import java.io.File

import org.apache.log4j.LogManager
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.catalyst.expressions.aggregate._

import scala.collection.parallel.ForkJoinTaskSupport


object AppHiveMetaExtractor {
  println( "Extracting Hive Meta Table and Column structures" )

  val spark: SparkSession =
    SparkSession
      .builder()
      .appName("Extracting Hive Meta Table and Column structures")
      .config("spark.master", "local")
      .enableHiveSupport()
      .getOrCreate()

  //

  import spark.implicits._
  import org.apache.spark.sql.functions._

  def main(args: Array[String]): Unit = {

    val log = LogManager.getRootLogger
    val startTime = System.currentTimeMillis()

    log.info("HYZ - " + spark.conf.get("spark.app.name") + " started. Duration: " + (System.currentTimeMillis() - startTime) / 1000 + " seconds")
    if (args.isEmpty) {
      println("Targetlocation parameter is missing for exporting metadata")
      System.exit(1)
    }

    val targetLocation = args(0)
    val showInd = args(1).toInt
    val showNum = args(2).toInt
    val logInd = args(3).toInt
    val parNum = args(4).toInt
    val dbName = if (args.size > 5 ) args(5).toString else "N.A."

    val forkJoinTaskSupportConfig = new scala.concurrent.forkjoin.ForkJoinPool(parNum)


    if (logInd == 0) {
      spark.sparkContext.setLogLevel("ERROR")
    }

    spark.sql("use default")

    /*
    spark.sql("CREATE external TABLE IF NOT EXISTS transaction_ext_txt (transaction_id INT, transaction_name STRING, transaction_desc STRING, transaction_cd VARCHAR(50)) PARTITIONED BY (reporting_dt timestamp) " +
      "ROW FORMAT DELIMITED " +
      "FIELDS TERMINATED BY '\\t' " +
      "STORED AS TEXTFILE " +
      "LOCATION '/export/data/default/transaction_ext_txt'")
    */

    //spark.sql("CREATE DATABASE IF NOT EXISTS landing")
    //spark.sql("CREATE TABLE IF NOT EXISTS landing.customer (customer_id INT, customer_name STRING) USING hive")
    //spark.sql("CREATE TABLE IF NOT EXISTS landing.product (product_id INT, product_name STRING, product_desc STRING, product_cd VARCHAR(50)) USING hive")
    //spark.sql("CREATE TABLE IF NOT EXISTS landing.organization (organization_id INT, organization_name STRING, organization_desc STRING, organization_cd VARCHAR(50)) USING hive")
    //spark.sql("CREATE TABLE IF NOT EXISTS landing.transaction (transaction_id INT, transaction_name STRING, transaction_desc STRING, transaction_cd VARCHAR(50)) PARTITIONED BY (reporting_dt timestamp)")

    /*
    for( seq <- 11 to 96){
      println( "Value of a: " + seq )
      spark.sql(s"CREATE TABLE IF NOT EXISTS landing.transaction_${seq} (transaction_id INT, transaction_name STRING, transaction_desc STRING, transaction_cd VARCHAR(50)) PARTITIONED BY (reporting_dt timestamp)")
    }
    */

    log.info("HYZ - metatables creation started. Duration: " + (System.currentTimeMillis() - startTime) / 1000 + " seconds")

    spark.sql("DROP TABLE IF EXISTS all_tables")
    spark.sql("CREATE TABLE IF NOT EXISTS all_tables (owner STRING, table_name STRING, table_type STRING, table_provider STRING, table_properties STRING, table_location STRING, table_serde_properties STRING, table_storage_properties STRING) ")
    spark.sql("TRUNCATE TABLE all_tables")

    spark.sql("DROP TABLE IF EXISTS all_table_columns")
    spark.sql("CREATE TABLE IF NOT EXISTS all_table_columns (owner STRING, table_name STRING, column_name STRING, column_order_id INT, data_type STRING, comment STRING, partition_column_ind VARCHAR(1)) ")
    spark.sql("TRUNCATE TABLE all_table_columns")

    log.info("HYZ - metatables creation finished. Duration: " + (System.currentTimeMillis() - startTime) / 1000 + " seconds")

    var databaseList = spark.sql("show databases")
    if (dbName != "N.A.") {
      databaseList = databaseList.filter($"databaseName" === dbName)
    }

    log.info("HYZ - Dbfilter: " + dbName + "Duration: " + (System.currentTimeMillis() - startTime) / 1000 + " seconds")

    databaseList.show(showNum)


    databaseList.collect().map({
      db => {
        val currentDB = db.getString(0)
        log.info("HYZ - metatables population for DB: " + currentDB + " started. Duration: " + (System.currentTimeMillis() - startTime) / 1000 + " seconds")
        spark.sql(s"use ${currentDB}")

        val tableList = spark.sql("show tables")

        tableList.show(showNum)

        val tableListPar = tableList.collect().par
        tableListPar.tasksupport = new ForkJoinTaskSupport(forkJoinTaskSupportConfig)

        tableListPar.map{
          tb => {
            val own = tb.getString(0)

            log.info("HYZ - metatables population for DB: " + own + " and table started. Duration: " + (System.currentTimeMillis() - startTime) / 1000 + " seconds")

            if (!own.isEmpty) {

              val tbl = tb.getString(1)
              log.info("HYZ - metatables population for DB: " + own + " and table: " + tbl + " started. Duration: " + (System.currentTimeMillis() - startTime) / 1000 + " seconds")

              val columnList = spark.sql(s"show columns in ${tbl}").select($"col_name".as("column_name"))
                .withColumn("column_order_id", monotonically_increasing_id() + 1)

              if (columnList.count() != 0) {
                val columnListExtra = spark.sql(s"describe formatted ${tbl}")

                // Table details

                if (showInd == 1) {
                  println(own)
                  println(tbl)
                  columnListExtra.show(showNum)
                }



                var tblType = "NA"
                val tblTypeFilter = columnListExtra.filter($"col_name".like("Type")).select($"data_type")
                if (tblTypeFilter.count() != 0) {
                  tblType = tblTypeFilter.head().getString(0)
                }

                var tblProvider = "NA"
                val tblProviderFilter = columnListExtra.filter($"col_name".like("Provider")).select($"data_type")
                if (tblProviderFilter.count() != 0) {
                  tblProvider = tblProviderFilter.head().getString(0)
                }

                var tblProperties = "NA"
                val tblPropertiesFilter = columnListExtra.filter($"col_name".like("Table Properties")).select($"data_type")
                if (tblPropertiesFilter.count() != 0) {
                  tblProperties = tblPropertiesFilter.head().getString(0)
                }

                var tblLocation = "NA"
                val tblLocationFilter = columnListExtra.filter($"col_name".like("Location")).select($"data_type")
                if (tblLocationFilter.count() != 0) {
                  tblLocation = tblLocationFilter.head().getString(0)
                }

                var tblSerdeProperties = "NA"
                val tblSerdePropertiesFilter = columnListExtra.filter($"col_name".like("Serde Library")).select($"data_type")
                if (tblSerdePropertiesFilter.count() != 0) {
                  tblSerdeProperties = tblSerdePropertiesFilter.head().getString(0)
                }

                var tblStorageProperties = "NA"
                val tblStoragePropertiesFilter = columnListExtra.filter($"col_name".like("Storage Properties")).select($"data_type")
                if (tblStoragePropertiesFilter.count() != 0) {
                  tblStorageProperties = tblStoragePropertiesFilter.head().getString(0)
                }

                spark.sql(s"insert into default.all_tables select '${own}' as owner, '${tbl}' as table_name, '${tblType}' as table_type, '${tblProvider}' as table_provider, '${tblProperties}' as table_properties, '${tblLocation}' as table_location, '${tblSerdeProperties}' as table_serde_properties, '${tblStorageProperties}' as table_storage_properties")

                // Column details


                val columnListJoined = columnListExtra.join(columnList, columnListExtra("col_name") === columnList("column_name"), "left")

                val realCols = columnListJoined.filter($"column_name" isNotNull).select($"col_name", $"data_type", $"comment", $"column_order_id")
                  .groupBy("col_name", "data_type", "comment", "column_order_id").agg(count("col_name").as("num_of_col"))
                  .withColumn("partition_column_ind", when($"num_of_col" === 1, "N").otherwise("Y"))
                  .withColumn("table_name", lit(tbl))
                  .withColumn("owner", lit(own))
                  .createOrReplaceTempView("tmp_load_all_table_columns")

                spark.sql("insert into default.all_table_columns select owner, table_name, col_name, column_order_id, data_type, comment, partition_column_ind from tmp_load_all_table_columns order by owner, table_name, column_order_id")
              } else {
                spark.sql(s"insert into default.all_tables select '${own}' as owner, '${tbl}' as table_name, 'XML' as table_type, 'NA' as table_provider, 'NA' as table_properties, 'NA' as table_location, 'NA' as table_serde_properties, 'NA' as table_storage_properties")
              }

            }
            tb.getString(0)
          }
        }
        log.info("HYZ - metatables population for DB: " + currentDB + " finished. Duration: " + (System.currentTimeMillis() - startTime) / 1000 + " seconds")
        db.getString(0)
      }

    })

    log.info("HYZ - metatables export started. Duration: " + (System.currentTimeMillis() - startTime) / 1000 + " seconds")
    spark.sql("select * from default.all_tables").repartition(1).write.option("delimiter", ";").option("header", "true").mode("overwrite").csv(targetLocation + "/" + startTime.toString + "/metadata/all_tables")
    spark.sql("select * from default.all_table_columns").repartition(1).write.option("delimiter", ";").option("header", "true").mode("overwrite").csv(targetLocation + "/" + startTime.toString + "/metadata/all_table_columns")
    log.info("HYZ - metatables export finished. Duration: " + (System.currentTimeMillis() - startTime) / 1000 + " seconds")

  }

}
