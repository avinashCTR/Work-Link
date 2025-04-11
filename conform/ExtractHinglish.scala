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

object ExtractHinglish extends BaseBlocks {

  var synonyms,hinglish: DataFrame = _

  def load(): Unit = {
    synonyms = ParquetToDF.getDF(setInputPath("synonyms"))
  }

  def doTransformations(): Unit = {
    hinglish=synonyms
      .filter(col("variant")==="HINGLISH")
      .select(lower(col("synonyms")).as("Word"),
              col("keyword").as("Hinglish Translation"), 
              lit("1").cast(StringType).as("is_hinglish")
             )
      .coalesce(1)
  }

  def save(): Unit = {

    var base_path = setOutputPath("hinglish", "Hinglish")
    
    DFToCSV.putDF(base_path,hinglish)

    var part_file_name = getListOfFiles(base_path)
    .filter(_.getName != "_SUCCESS").map(x=>x.toString.split("/").last).toSeq.head

    rename(base_path+"/"+part_file_name, base_path+"/"+"HinglishVariants.csv")
    
  }
}