package spark.predict

import org.apache.spark.mllib.linalg.{Vector, Vectors}

/**
 * TODO: Explain This
 *
 * Created by hwang on 5/21/15.
 */
object buildPredictSet {
  def build_predict_set(d:Array[Array[String]], m:Array[String]): Vector = {
    val values = Array.fill(m.size)(0)
    for(element <- d){
      if(m.contains(element(1)))
        values(m.indexOf(element(1))) = if(compareWords(element(0), element(1))) -1 else 1
    }
    Vectors.dense(values.map(_.toDouble))
  }

  def compareWords(a: String, b: String): Boolean ={
    if(a.split("\\W")(0) == b.split("\\W")(0))
      true
    else
      false
  }
}
