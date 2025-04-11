package ai.couture.obelisk.search.etl.jiomart.conform

import ai.couture.obelisk.commons.io.{DFToCSV,CSVToDF,ParquetToDF}
import ai.couture.obelisk.commons.io.HdfsUtils.{getListOfFiles, copy, rename}
import ai.couture.obelisk.commons.utils.BaseBlocks
import ai.couture.obelisk.search.Constants._
import ai.couture.obelisk.search.Schemas.{categorySchemaFloat, categoryStructFloat}
import org.apache.spark.sql.expressions.{UserDefinedFunction, Window}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{FloatType, IntegerType, StringType}
import org.apache.spark.sql.{DataFrame, Row}

object ExtractBrandAcronyms extends BaseBlocks {
  
    var synonyms,branddata: DataFrame = _

    def load(): Unit = {
        synonyms = ParquetToDF.getDF(setInputPath("synonyms"))
    }

    def doTransformations(): Unit = {
        branddata=synonyms
        .filter(col("variant")==="BRAND ACRONYM")
        .select(lower(col("synonyms")).as("phrase"),
                col("keyword").as("phrase_key"), 
                lit("1").cast(StringType).as("is_acronym")
                )
        .coalesce(1)
    }

    def save(): Unit = {

        var base_path = setOutputPath("brand_acronyms", "BrandAcronyms")

        DFToCSV.putDF(base_path,branddata)

        var part_file_name = getListOfFiles(base_path)
        .filter(_.getName != "_SUCCESS").map(x=>x.toString.split("/").last).toSeq.head

        rename(base_path+"/"+part_file_name, base_path+"/"+"BrandAcronyms.csv")
    }
}
