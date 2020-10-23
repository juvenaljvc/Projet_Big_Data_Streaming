import SparkBigData._
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.hadoop.fs._
import org.apache.hadoop.conf._

object UseCaseBANO {

  val schema_bano = StructType(Array(
      StructField("id_bano", StringType, false),
      StructField("numero_voie", StringType, false),
      StructField("nom_voie", StringType, false),
      StructField("code_postal", StringType, false),
      StructField("nom_commume", StringType, false),
      StructField("code_source_bano", StringType, false),
      StructField("latitude", StringType, true),
      StructField("longitude", StringType, true)
  ))

  val configH = new Configuration()
  val fs = FileSystem.get(configH)

  val chemin_dest = new Path("F:\\Mes publications\\Mes publications\\Complete\\formation Spark Big Data\\Ressources\\Sources")

  def main(args: Array[String]): Unit = {

    val ss = Session_Spark(true)

    val df_bano_brut = ss.read
      .format("com.databricks.spark.csv")
      .option("delimiter", ",")
      .option("header", "true")
      .schema(schema_bano)
      .csv("F:\\Mes publications\\Mes publications\\Complete\\formation Spark Big Data\\Ressources\\Sources\\BANO complet csv\\Bano full\\full.csv")

   // df_bano_brut.show(10)

    val df_bano = df_bano_brut
      .withColumn("code_departement", substring(col("code_postal"), 1, 2))
      .withColumn("libelle_source", when(col("code_source_bano") === lit("OSM"), lit("OpenStreeMap"))
      .otherwise(when(col("code_source_bano") === lit("OD"), lit("OpenData"))
      .otherwise(when(col("code_source_bano") === lit("O+O"), lit("OpenData OSM"))
      .otherwise(when(col("code_source_bano") === lit("CAD"), lit("Cadastre"))
      .otherwise(when(col("code_source_bano") === lit("C+O"), lit("Cadastre OSM")))))))

   // df_bano.show(10)

    val df_departement = df_bano.select(col("code_departement")).distinct().filter(col("code_departement").isNotNull)

    val liste_departement = df_bano.select(col("code_departement"))
      .distinct()
      .filter(col("code_departement").isNotNull)
      .collect()
      .map(x => x(0)).toList

  //  liste_departement.foreach(e => println (e.toString))

    liste_departement.foreach{
      x => df_bano.filter(col("code_departement") === x.toString)
          .coalesce(1)
          .write
          .format("com.databricks.spark.csv")
          .option("delimiter", ";")
          .option("header", "true")
          .mode(SaveMode.Overwrite)
          .csv("F:\\Mes publications\\Mes publications\\Complete\\formation Spark Big Data\\Ressources\\Sources\\BANO complet csv\\bano" + x.toString)

        val chemin_source = new Path("F:\\Mes publications\\Mes publications\\Complete\\formation Spark Big Data\\Ressources\\Sources\\BANO complet csv\\bano" + x.toString)
        fs.copyFromLocalFile(chemin_source, chemin_dest)
    }

    df_departement.foreach{
      dep =>  df_bano.filter(col("code_departement") === dep.toString())
        .repartition(1)
        .write
        .format("com.databricks.spark.csv")
        .option("delimiter", ";")
        .option("header", "true")
        .mode(SaveMode.Overwrite)
        .csv("F:\\Mes publications\\Mes publications\\Complete\\formation Spark Big Data\\Ressources\\Sources\\BANO complet csv\\bano" + dep.toString)

        val chemin_source = new Path("F:\\Mes publications\\Mes publications\\Complete\\formation Spark Big Data\\Ressources\\Sources\\BANO complet csv\\bano" + dep.toString)
        fs.copyFromLocalFile(chemin_source, chemin_dest)

    }


  }


}
