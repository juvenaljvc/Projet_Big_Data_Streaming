import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.scalatest.flatspec.AnyFlatSpec
import org.apache.spark.sql.functions.lit

trait SparkSessionProvider {
  val sst = SparkSession.builder
    .master("local[*]")
    .config("spark.sql.crossJoin.enabled", "true")
    .getOrCreate()
}

class SparkTestsUnitaires extends AnyFlatSpec with SparkSessionProvider  {

  // test de la session Spark
  it should("create a Spark Session") in {
    var environement : Boolean = true
    val sst = SparkBigData.Session_Spark(environement)
  }

  it should("compare 2 data frame") in {
   val data_schema = List(
     StructField("employe", StringType, true),
     StructField("salaire", IntegerType, true)
   )

    val data_source = Seq(
      Row("Juvenal", 140000),
      Row("Sabine", 200000),
      Row("Angel", 150000)
    )

    val df : DataFrame = sst.createDataFrame(
      sst.sparkContext.parallelize(data_source),
      StructType(data_schema)
    )

    df.show(3)

    val df_nouveau : DataFrame = df.withColumn("salaire", lit(10000))
    df_nouveau.show(3)

    /// assert(df.columns.size === df_nouveau.columns.size)
    /// assert(df.count() === 3)
    assert(df_nouveau.take(2).length === 15)

  }

}
