package ai.couture.obelisk.search.etl.jiomart.conform

import ai.couture.obelisk.commons.io.{DFToParquet, ParquetToDF}
import ai.couture.obelisk.commons.utils.BaseBlocks
import ai.couture.obelisk.search.Constants._
import ai.couture.obelisk.search.Schemas.{categorySchemaFloat, categoryStructFloat}
import org.apache.spark.sql.expressions.{UserDefinedFunction, Window}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.{DataFrame, Row}

object BrandAcronyms extends BaseBlocks {
  
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
        DFToParquet.DFToParquet(branddata, setOutputPath("brand_acronyms"))
    }
}
