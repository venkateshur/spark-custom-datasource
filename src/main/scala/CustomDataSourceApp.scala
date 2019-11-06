import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

object CustomDataSourceApp extends App {


  val conf = new SparkConf().setAppName("spark-custom-datasource")

  val spark = SparkSession.builder().config(conf).master("local").getOrCreate()

  val df = spark.read.format("io.custom.datasource.cf").load("data/")

  // Step 1 (Schema verification)
  df.schema.printTreeString()
  // Step 2 (Read data)
  df.show()
  // Step 3 (Write data)
  df.write
    .options(Map("format" -> "cf"))
    .mode(SaveMode.Overwrite)
    .format("io.custom.datasource.cf")
    .save("out/")



}

