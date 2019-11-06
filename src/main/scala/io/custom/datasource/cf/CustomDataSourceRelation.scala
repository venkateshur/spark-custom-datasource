package io.custom.datasource.cf

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._

import scala.util.matching.Regex
/**
  * Created by rana on 29/9/16.
  */
class CustomDataSourceRelation(override val sqlContext : SQLContext, path : String, userSchema : StructType)
  extends BaseRelation with TableScan with PrunedScan with PrunedFilteredScan with Serializable {

  override def schema: StructType = {
    if (userSchema != null) {
      userSchema
    } else {
      StructType(
          StructField("name", StringType, true) ::
          StructField("skill", StringType, true) ::
          StructField("dept", StringType, true) ::
          StructField("id", IntegerType, true) :: Nil
      )
    }
  }

  override def buildScan(): RDD[Row] = {
    val initialRdd = sqlContext.sparkContext.wholeTextFiles(path).map(_._2)
    val schemaFields = schema.fields

    val rowsRdd = initialRdd.map(fileContent => {
      val lines = fileContent.split("\n")
      val data = lines.map(line => line.split(Regex.quote(",")).map(word => word.trim).toSeq)

      val records = data.map(words => words.zipWithIndex.map {
        case (value, index) =>

          val columnType = schemaFields(index).dataType
          val castValue = columnType match {
            case StringType => value
            case IntegerType => value.toInt
          }
          castValue
      })
      records.map(record => Row.fromSeq(record))
    })

    rowsRdd.flatMap(row => row)
  }

  override def buildScan(requiredColumns: Array[String]): RDD[Row] = {
    println("Selecting only required columns...")
    // An example, does not provide any specific performance benefits
    val initialRdd = sqlContext.sparkContext.wholeTextFiles(path).map(_._2)
    val schemaFields = schema.fields

    val rowsRdd = initialRdd.map(fileContent => {
      val lines = fileContent.split("\n")
      val data = lines.map(line => line.split(Regex.quote(",")).map(word => word.trim).toSeq)

      val records = data.map(words => words.zipWithIndex.map {
        case (value, index) =>
          val field = schemaFields(index)
          if (requiredColumns.contains(field.name)) Some(cast(value, field.dataType)) else None
      })

      records
        .map(record => record.filter(_.isDefined))
        .map(record => Row.fromSeq(record))
    })

    rowsRdd.flatMap(row => row)
  }


  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    // Nothing is actually pushed down, just iterate through all filters and print them
    println("Trying to push down filters...")
    filters foreach println
    buildScan(requiredColumns)
  }

  private def cast(value: String, dataType: DataType) = dataType match {
    case StringType => value
    case IntegerType => value.toInt
  }

}