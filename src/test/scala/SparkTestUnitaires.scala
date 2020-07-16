import SparkBigData.ss
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.scalatest.matchers.should.Matchers
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest._
import org.apache.spark.sql.functions.lit
import com.holdenkarau.spark.testing._

trait SparkSessionProvider {
  val sst = SparkSession.builder
    .master("local[*]")
    .getOrCreate()
}

class SparkTestUnitaires extends AnyFlatSpec with SparkSessionProvider with DataFrameSuiteBase {

  it should("instanciate a Spark Session") in {

    var env : Boolean = true
    val sst = SparkBigData.Session_Spark(env)

  }

  it should("compare two data frame") in {
    val structure_df = List(
      StructField("Employe", StringType, true),
     StructField("Salaire", IntegerType, true)
    )

    val data_df = Seq(
      Row("Juvenal", 140000),
      Row("Sabine", 200000),
      Row( "Angel", 150000)
    )

    val df_source : DataFrame = sst.createDataFrame(
      sst.sparkContext.parallelize(data_df),
      StructType(structure_df)
    )

    df_source.show()

    val df_new : DataFrame = df_source.withColumn("Salaire", lit(100000))
    df_new.show()

    // assert(df_source.columns.size === df_new.columns.size)
    // assert(df_source.count() === 3)
    assert(df_source.take(3).length === 15)
    assertDataFrameEquals(df_source, df_new)

  }


}
