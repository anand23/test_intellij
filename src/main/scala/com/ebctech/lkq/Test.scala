package com.ebctech.lkq

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession


class Test {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark: SparkSession = SparkSession.builder()
    .master("local[*]")
    .appName("com.ebctech.lkq.Test-Local")
    .getOrCreate

  val test = spark.read.format("csv")
    .options(Map("header" -> "true", "delimiter" -> ","))
    .load("/home/spineor/Desktop/test")

  test.printSchema()

  println(test.count())

  spark.time(test.count())

}

object Test {
  def apply(): Test = new Test()
}