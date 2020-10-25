from typing import List

from pyspark.sql import DataFrame


def get_table_schema_from_df(df: DataFrame, db_name: str, table_name: str) -> str:

    COLUMN_TYPE_DICT = {

        "string": "text",
        "int": "int",
        "double": "double",
        "date": "date",
        "timestamp": "datetime"
    }

    create_table_statement: str = f"    CREATE TABLE IF NOT EXISTS {db_name}.{table_name} ( \n\n"
    columns_definitions: List[str] = list(map(lambda x: f"      {x[0]} {COLUMN_TYPE_DICT[x[1]]}", df.dtypes))
    return create_table_statement + ",\n".join(columns_definitions) + " )"
