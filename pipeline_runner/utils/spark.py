from typing import List

from pyspark.sql import DataFrame


def df_print_schema(df: DataFrame) -> str:

    schema_json: dict = df.schema.jsonValue()
    schema_str_list: List[str] = list(map(lambda x:
                                          f" |-- {x['name']}: {x['type']} (nullable: {str(x['nullable']).lower()})",
                                          schema_json["fields"]))
    schema_str_list.insert(0, "\nroot")
    schema_str_list.append("\n")

    return "\n".join(schema_str_list)