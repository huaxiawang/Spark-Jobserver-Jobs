package spark.predict

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.mllib.regression.LinearRegressionModel
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.SparkContext
import spark.jobserver.{SparkJobInvalid, SparkJobValid, SparkJobValidation, SparkJob}
import spark.predict.buildPredictSet.build_predict_set

import scala.util.Try

/**
 * TODO: Explain This
 *
 * Created by hwang on 5/28/15.
 */

object predictJob extends SparkJob{

  def main (args: Array[String]) {
    val sc = new SparkContext("spark://localhost:7077", "predict job")
    val config = ConfigFactory.parseString("")
    val result = runJob(sc, config)
    println("Result is " + result)
  }

  override def runJob(sc: SparkContext, jobConfig: Config): Any = {
    val sqlContext = new HiveContext(sc)
    val specialty = jobConfig.getString("input.specialty")
    val state = jobConfig.getString("input.state")
    val queryString = "select " +
      "NPPES_PROVIDER_LAST_ORG_NAME, NPPES_PROVIDER_FIRST_NAME, DRUG_NAME, GENERIC_NAME, NPPES_PROVIDER_CITY " +
      "from prescription " +
      s"where NPPES_PROVIDER_STATE='$state' " +
      s"and SPECIALTY_DESC='$specialty'"
    val df = sqlContext.sql(queryString)

    val medicine_list = sqlContext.sql(s"select GENERIC_NAME from prescription where SPECIALTY_DESC='$specialty'").map(row => row(0).toString).distinct().collect()
    val model = LinearRegressionModel.load(sc, s"hdfs://localhost:9000/user/hwang/training/${specialty.replaceAll("[^A-Za-z0-9]", "")}_model")

    val predictData = df.map(row => (row(4).toString, s"${row(0).toString} ${row(1).toString}", Array(row(2).toString, row(3).toString)))
      .groupBy(_._1).mapValues(values => values.map(v => (v._2, v._3))
      .groupBy(_._1).map{case(k, v) => (k, build_predict_set(v.toArray.map(_._2), medicine_list))})

    Map(state -> predictData.map{case(city, data_map) => (city, data_map.map{case(k, v) => (k, model.predict(v))})}.collect().toMap)
  }

  override def validate(sc: SparkContext, config: Config): SparkJobValidation = {
    Try(config.getString("input.state") + config.getString("input.specialty"))
      .map(x => SparkJobValid)
      .getOrElse(SparkJobInvalid("No input.state or input.specialty config param"))
  }
}
