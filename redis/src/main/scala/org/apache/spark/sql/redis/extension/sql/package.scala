package org.apache.spark.sql.redis.extension

import scala.language.implicitConversions

import org.apache.spark.sql.DataFrame

package object sql {

  // the sparkDatasetFunctions already takes care of this
  // but older clients might still import it hence why it's still here
  implicit def sparkDataFrameFunctions(df: DataFrame) = new SparkDataFrameFunctions(df)

  class SparkDataFrameFunctions(df: DataFrame) extends Serializable {
    def saveToRedis(cfg: scala.collection.Map[String, String]): Unit = { RedisSparkSql.saveToRedis(df, cfg) }
  }
}
