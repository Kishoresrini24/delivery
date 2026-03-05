from pyspark.sql import DataFrame
import pyspark.sql.functions as F


def apply(df: DataFrame) -> DataFrame:
    df_mapped = df.withColumns(
        {
            "is_company_valid_flag": F.when(F.col("IS_COMPANY_VALID") == True, 1).otherwise(0)
        }
    )

    df_aggregated = (
        df_mapped.groupBy("PUBLICATION_NUMBER")
        .agg(F.sum("is_company_valid_flag").alias("keep_intact"))
        .filter(F.col("keep_intact") == 0)
    )

    return df_aggregated