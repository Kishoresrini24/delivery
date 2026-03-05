from pyspark.sql import DataFrame
from pyspark.sql import functions as F

import src.declarations.models.gold.declarations.declaration_gold as DeclarationGoldModel


def apply(df: DataFrame) -> DataFrame:
    return (
        df.filter(F.col("standard_project").isNotNull())
        .filter(F.col("standard_document_ref").isNotNull())
        .filter((F.col("group") != "") & F.col("group").isNotNull())
        .dropDuplicates(["standard_document_ref"])
        .select(
            DeclarationGoldModel.STANDARD_PROJECT,
            DeclarationGoldModel.STANDARD_PROJECT_SEARCH,
            F.col("harmonized_standard_doc_id"),
            F.col("standard_document_ref"),
            F.col("standard_document_id_harmonized_search"),
            F.col("standard_document_id_harmonized"),
            F.col("original_standard"),
            F.col("standard_document_id_search"),
            DeclarationGoldModel.STANDARD_DOCUMENT_ID,
        )
        .distinct()
    )