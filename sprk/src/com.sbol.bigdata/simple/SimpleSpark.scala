package com.sbol.bigdata

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.DataTypes

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

}
