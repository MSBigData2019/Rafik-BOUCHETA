package com.sparkProject

import org.apache.spark.SparkConf
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.evaluation.{MulticlassClassificationEvaluator, RegressionEvaluator}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.ml.feature._
import org.apache.spark.ml.tuning.{ParamGridBuilder, TrainValidationSplit, TrainValidationSplitModel}



object Trainer {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAll(Map(
      "spark.scheduler.mode" -> "FIFO",
      "spark.speculation" -> "false",
      "spark.reducer.maxSizeInFlight" -> "48m",
      "spark.serializer" -> "org.apache.spark.serializer.KryoSerializer",
      "spark.kryoserializer.buffer.max" -> "1g",
      "spark.shuffle.file.buffer" -> "32k",
      "spark.default.parallelism" -> "12",
      "spark.sql.shuffle.partitions" -> "12",
      "spark.driver.maxResultSize" -> "2g"
    ))

    val spark = SparkSession
      .builder
      .config(conf)
      .appName("TP_spark")
      .getOrCreate()

    import spark.implicits._

    /*******************************************************************************
      *
      *       TP 3
      *
      *       - lire le fichier sauvegarder précédemment
      *       - construire les Stages du pipeline, puis les assembler
      *       - trouver les meilleurs hyperparamètres pour l'entraînement du pipeline avec une grid-search
      *       - Sauvegarder le pipeline entraîné
      *
      *       if problems with unimported modules => sbt plugins update
      *
      ********************************************************************************/

    println("hello world ! from Trainer")

    /** 1 - CHARGEMENT DES DONNEES **/

    val df:DataFrame = spark.read.parquet("./src/main/resources/prepared_trainingset")
    df.show(10)

    /** 2 - DONNEES TEXT **/
    //2-a)

    val tokenizer = new RegexTokenizer()
      .setPattern("\\W+")
      .setGaps(true)
      .setInputCol("text")
      .setOutputCol("tokens")

    //2-b)
    val remover = new StopWordsRemover().setInputCol("tokens").setOutputCol("tokensClean")

    // 2-c)
    val vectorize = new CountVectorizer().setInputCol("tokensClean").setOutputCol("vectorize")

    // 2-d)
    val tfidf = new IDF().setInputCol("vectorize").setOutputCol("tfidf")


    /** 3 Convertir les catégories en données numériques **/
    //3-e)
    val indexerCountry = new StringIndexer().setInputCol("country2").setOutputCol("country_indexed").setHandleInvalid("skip")

    //3-f)
    val indexerCurrency = new StringIndexer().setInputCol("currency2").setOutputCol("currency_indexed").setHandleInvalid("skip")

    //3-g)
    val encoder = new OneHotEncoderEstimator().setInputCols(Array("country_indexed", "currency_indexed")).setOutputCols(Array("countryVect", "currencyVect"))

    /** 4 Mettre les données sous une forme utilisable par Spark.ML **/
    //4-h)
    val assembler = new VectorAssembler().setInputCols(Array("tfidf", "days_campaign", "hours_prepa",
      "goal", "country_indexed", "currency_indexed")).setOutputCol("features")

    //4-i)
    val lr = new LogisticRegression()
      .setElasticNetParam(0.0)
      .setFitIntercept(true)
      .setFeaturesCol("features")
      .setLabelCol("final_status")
      .setStandardization(true)
      .setPredictionCol("predictions")
      .setRawPredictionCol("raw_predictions")
      .setTol(1.0e-6)
      .setMaxIter(300)

    //4-j)
    val pipeline = new Pipeline().setStages(Array(tokenizer, remover, vectorize, tfidf, indexerCountry, indexerCurrency,
                                                  encoder, assembler, lr))
    /** 5 Entraînement et tuning du modèle **/
    //5-k)
    val Array(training, test) = df.randomSplit(Array(0.9, 0.1), seed = 2810)


    //5-l
    val paramGrid = new ParamGridBuilder().addGrid(lr.regParam, 10e-8 to 10e-2 by 2.0)
        .addGrid( vectorize.minDF, 55.0 to 95.0 by 20.0).build()

    val evaluator = new MulticlassClassificationEvaluator().setLabelCol("final_status")
      .setPredictionCol("predictions").setMetricName("f1")

    val trainValidationSplit = new TrainValidationSplit()
      .setEstimator(pipeline)
      .setEvaluator(evaluator)
      .setEstimatorParamMaps(paramGrid)
      .setTrainRatio(0.7)

    //val model = trainValidationSplit.fit(training)

    // Now we can optionally save the fitted pipeline to disk
    //model.write.overwrite().save("./src/main/resources/tmp/spark-logistic-regression-model")

    // We can also save this unfit pipeline to disk
    //trainValidationSplit.write.overwrite().save("./src/main/resources/tmp/gridsearch-model")

    // And load it back in during production
    val sameModel = TrainValidationSplitModel.load("./src/main/resources/tmp/spark-logistic-regression-model")


    //5-m)
    val df_WithPredictions = sameModel.transform(test)

    print("f-score : "+evaluator.evaluate(df_WithPredictions)+"\n")

    //5-n)
    df_WithPredictions.groupBy("final_status", "predictions").count().show()


  }
}
