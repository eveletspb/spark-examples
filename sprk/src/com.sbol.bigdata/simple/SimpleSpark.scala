package com.sbol.bigdata

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SaveMode

object SimpleSpark extends App {

  val sparkSession = SparkSession
    .builder()
    .appName("simple spark application")
    .master("local[4]")
    .getOrCreate()

  import sparkSession.implicits._

  val ds: Dataset[Int] = Seq(1, 2, 3, 4, 5).toDS()

  ds.printSchema()

  val field = StructField(
    name = "value",
    dataType = DataTypes.IntegerType,
    nullable = false,
    metadata = null
  )
  val schema = StructType(fields = Array(field))

  val produceFunc: (Int) => Int = { it => it * 2 }

  val producedDs = ds.map(produceFunc)

  producedDs.show()

  val seq = Seq("This post describe basic", "about", "Spark Scala").toDS()

  val flatMapDs = seq.flatMap({ s => s.split(" ") })

  flatMapDs.show()

  val filteredDs = flatMapDs.filter { s => s == "Scala" }

  filteredDs.show()

  flatMapDs.count()

  val resultList = flatMapDs.collect()

  val personsDs: Dataset[Person] = Seq(
    Person("Alice", 2000, 25, 2024),
    Person("Tom", 4000, 20, 2025),
    Person("Jonh", 6000, 17, 2020),
    Person("Vladimir", 9999, 29, 2099)
  ).toDS()

  val averageSalary = personsDs
    .agg(avg("salary").as("avg_salary"))
    .as[Double]
    .show()

  val maxAge = personsDs
    .agg(max("age").as("max_age"))
    .as[Double]
    .show()

  val averageAge2 = personsDs
    .select(
      avg("salary").as("avg_salary")
    )
    .as[Double]
    .show()

    personsDs.write
    .mode(SaveMode.Overwrite)
    .partitionBy("year")
    .parquet("dataset/parquet")

}

case class Person(name: String, salary: Double, age: Int, year: Int)
