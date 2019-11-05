package io.github.interestinglab.waterdrop.output.batch

import com.typesafe.config.{Config, ConfigFactory}
import io.github.interestinglab.waterdrop.apis.BaseOutput
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.redis.extension.sql._
import scala.collection.JavaConversions._

/**
  * Redis batch output
  */
class Redis extends BaseOutput {

  var config: Config = ConfigFactory.empty()

  var redisCfg: Map[String, String] = Map()

  /**
    * Set Config.
    * */
  override def setConfig(config: Config): Unit = {
    this.config = config
  }

  /**
    * Get Config.
    * */
  override def getConfig(): Config = {
    this.config
  }

  override def checkConfig(): (Boolean, String) = {
    config.hasPath("hosts") && config.getStringList("hosts").size() > 0 match {
      case true => {
        val hosts = config.getStringList("hosts")
        // TODO CHECK hosts
        (true, "")
      }
      case false => (false, "please specify [hosts] as a non-empty string list")
    }
  }


  override def prepare(spark: SparkSession): Unit = {
    super.prepare(spark)
    config.entrySet().foreach(e => {
      val key = e.getKey
      val value = String.valueOf(e.getValue.unwrapped())
      redisCfg += (key -> value)
    })
    println("[INFO] Output Redis Params:")
    for (entry <- redisCfg) {
      val (key, value) = entry
      println("[INFO] \t" + key + " = " + value)
    }
  }

  override def process(df: Dataset[Row]): Unit = {
    df.saveToRedis(redisCfg)
  }
}
