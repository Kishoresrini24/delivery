from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, BooleanType, StringType

import src.declarations.models.gold.declarations.declaration_gold as DeclarationGoldModel


def apply(df: DataFrame) -> DataFrame:
    return (
        df.withColumn(DeclarationGoldModel.ESSENTIAL_CLAIM_NO, F.lit(None).cast(StringType()))
        .withColumn(DeclarationGoldModel.ORIGINAL_STANDARD, F.lit(None).cast(StringType()))
        .withColumn(DeclarationGoldModel.POOL_NAME_HARMONIZED, F.lit(None).cast(StringType()))
        .withColumn(DeclarationGoldModel.POOL_PROVIDER, F.lit(None).cast(StringType()))
        .withColumn(DeclarationGoldModel.STANDARD_DOCUMENT_REF, F.lit(None).cast(StringType()))
        .withColumn(DeclarationGoldModel.TECHNOLOGY_CLASSIFICATION, F.lit(None).cast(StringType()))
        .withColumn(
            DeclarationGoldModel.TECHNOLOGY_CLASSIFICATION_ARRAY,
            F.lit(None).cast(ArrayType(StringType())),
        )
        .drop("darts_ip_url")
        .drop("pooled")
        .withColumn(DeclarationGoldModel.POOLED, F.lit(False).cast(BooleanType()))
    )