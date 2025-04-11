package ai.couture.obelisk.search.etl.jiomart.conform

import ai.couture.obelisk.commons.io.{DFToCSV,CSVToDF,DFToParquet,ParquetToDF}
import ai.couture.obelisk.commons.io.HdfsUtils.{getListOfFiles, copy, rename}
import ai.couture.obelisk.commons.utils.BaseBlocks
import ai.couture.obelisk.search.Constants._
import ai.couture.obelisk.search.Schemas.{categorySchemaFloat, categoryStructFloat}
import org.apache.spark.sql.expressions.{UserDefinedFunction, Window}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{FloatType, IntegerType, StringType}
import org.apache.spark.sql.{DataFrame, Row}

object ExtractSynonymsUKUS extends BaseBlocks {

  var synonyms, ukus: DataFrame = _

  def load(): Unit = {
    synonyms = ParquetToDF.getDF(setInputPath("synonyms"))
  }

  def doTransformations(): Unit = {
    ukus = synonyms
      .filter(col("variant")==="UK TO US")
      .select(lower(col("keyword")).as("uk"), lower(col("synonyms")).as("american"))
      .coalesce(1)
  }

  def save(): Unit = {

    val base_path = setOutputPath("synonyms_ukus","Synonyms_ukus")

    val base_synonyms = setOutputPath("synonyms_base_path","Synonyms") 

    DFToCSV.putDF(base_path,ukus)

    var part_file_name = getListOfFiles(base_path)
    .filter(_.getName != "_SUCCESS").map(x=>x.toString.split("/").last).toSeq.head

    rename(base_path+"/"+part_file_name, base_path+"/"+"uk-to-us-eng.csv")

    copy(base_path+"/"+"uk-to-us-eng.csv", base_synonyms+"/"+"uk-to-us-eng.csv")
  }
}