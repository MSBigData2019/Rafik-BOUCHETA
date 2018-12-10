package com.sparkProject

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.functions._


object Preprocessor {

  def main(args: Array[String]): Unit = {

    // Des réglages optionels du job spark. Les réglages par défaut fonctionnent très bien pour ce TP
    // on vous donne un exemple de setting quand même
    val conf = new SparkConf().setAll(Map(
      "spark.scheduler.mode" -> "FIFO",
      "spark.speculation" -> "false",
      "spark.reducer.maxSizeInFlight" -> "48m",
      "spark.serializer" -> "org.apache.spark.serializer.KryoSerializer",
      "spark.kryoserializer.buffer.max" -> "1g",
      "spark.shuffle.file.buffer" -> "32k",
      "spark.default.parallelism" -> "12",
      "spark.sql.shuffle.partitions" -> "12"
    ))

    // Initialisation de la SparkSession qui est le point d'entrée vers Spark SQL (donne accès aux dataframes, aux RDD,
    // création de tables temporaires, etc et donc aux mécanismes de distribution des calculs.)
    val spark = SparkSession
      .builder
      .config(conf)
      .appName("TP_spark")
      .getOrCreate()

    import spark.implicits._

    val sc = spark.sparkContext


    /** *****************************************************************************
      *
      * TP 2
      *
      *       - Charger un fichier csv dans un dataFrame
      *       - Pre-processing: cleaning, filters, feature engineering => filter, select, drop, na.fill, join, udf, distinct, count, describe, collect
      *       - Sauver le dataframe au format parquet
      *
      * if problems with unimported modules => sbt plugins update
      *
      * *******************************************************************************/

    println("hello world ! from Preprocessor")

    // a
    val df = spark.read.format("csv").option("header", "true").load("/Users/rafman/Desktop/BGD/INF729-Spark-Hadoop/Spark/Tp2/train_clean.csv")
    // b
    println("nombre de ligne :" + df.count())
    println("nb columns : " + df.columns.size)
    //c

    df.show(10)

    // d

    df.printSchema()

    // e

    val df2 = df.withColumn("deadline", df("deadline").cast(IntegerType))
      .withColumn("state_changed_at", df("state_changed_at").cast(IntegerType))
      .withColumn("created_at", df("created_at").cast(IntegerType))
      .withColumn("launched_at", df("launched_at").cast(IntegerType))
      .withColumn("backers_count", df("backers_count").cast(IntegerType))
      .withColumn("final_status", df("final_status").cast(IntegerType))
      .withColumn("goal", df("goal").cast(IntegerType))


    df2.printSchema()

    // 2.a
    df2.describe("state_changed_at", "created_at", "deadline", "launched_at", "backers_count", "final_status").show()

    // 2.b
    df2.groupBy("disable_communication").count.orderBy($"count".desc).show(100)
    df2.groupBy("country").count.orderBy($"count".desc).show(100)
    df2.groupBy("currency").count.orderBy($"count".desc).show(100)
    df2.select("deadline").dropDuplicates.show()
    df2.groupBy("state_changed_at").count.orderBy($"count".desc).show(100)
    df2.groupBy("backers_count").count.orderBy($"count".desc).show(100)
    df2.select("goal", "final_status").show(30)
    df2.groupBy("country", "currency").count.orderBy($"count".desc).show(50)

    // 2.c

    val df3 : DataFrame = df2.drop("disable_communication")
    df3.printSchema()

    // 2.d

    val dfNoFutur: DataFrame = df2.drop("backers_count", "state_changed_at")
    dfNoFutur.printSchema()

    // 2.e
    val dfCountry: DataFrame = dfNoFutur.withColumn("country2", when(condition=$"country"==="False", value=$"currency").otherwise($"country"))
              .withColumn("currency2", when(condition=$"country".isNotNull && length($"currency")=!=3, value=null).otherwise($"currency"))
              .drop("country", "currency")
    dfCountry.groupBy("country2", "currency2").count.orderBy($"count".desc).show(50)

    // 2.f
  }
}
