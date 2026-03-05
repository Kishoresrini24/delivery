from pyspark.sql import DataFrame

import src.declarations.models.silver.declarations.cellular_technology_classification as cellular_technology_classification


def apply(df: DataFrame) -> DataFrame:

    rename_map = {
        "harmonized_standard_doc_id": "harmonized_standard_doc_id_top10",
        "standard_document_id": "standard_document_id_top10",
        cellular_technology_classification.TECHNOLOGY_CLASSIFICATION: "technology_classification_cellular",
    }

    for old, new in rename_map.items():
        if old in df.columns:
            df = df.withColumnRenamed(old, new)

    return df