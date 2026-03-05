from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

import src.declarations.models.gold.declarations.declaration_gold as DeclarationGoldModel
from src.declarations.utils.declarations_id_generator_helper import DeclarationsIdGeneratorHelper


make_id_udf = F.udf(DeclarationsIdGeneratorHelper.make_id_for_declaration_raw, StringType())


def apply(df: DataFrame) -> DataFrame:
    return (
        df.filter(F.col("standard_document_ref").isNotNull())
        .withColumns(
            {
                DeclarationGoldModel.ID: make_id_udf(
                    F.col("standard_document_id"),
                    F.col("standard_project"),
                    F.initcap(F.col("declaring_company")),
                    F.date_format(F.col("declaration_date"), "yyyy-MM-dd"),
                    F.col("publication_number"),
                    F.concat_ws(" | ", F.col("isld")),
                ),
                "id_first_char": F.substring(F.col(DeclarationGoldModel.ID), 1, 1),
                "expanded": F.lit("Core TS expansion"),
            }
        )
        .withColumnRenamed("harmonized_group", "group")
    )