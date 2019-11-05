package org.apache.spark.sql.redis.extension

import org.apache.spark.sql.Dataset

import scala.collection.Map

object RedisSparkSql {

  //write
  def saveToRedis(srdd: Dataset[_], cfg: Map[String, String]): Unit = {
    if (srdd != null) {
      if (srdd.isStreaming) {
        throw new UnsupportedOperationException("Streaming Datasets should not be saved with 'saveToRedis()'. ")
      }
      srdd.write.format("org.apache.spark.sql.redis")
        .options(cfg)
        .save()
    }
  }
}
