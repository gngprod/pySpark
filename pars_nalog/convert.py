from pyspark.sql import DataFrame, functions as f
from pyspark.sql.types import *


def get_exploded(df: DataFrame):
    def get_cols_list(schema: StructType) -> list:
        res_list = []
        for field in schema:
            if isinstance(field.dataType, StructType):
                res_list += [".".join([field.name, str(col)]) for col in get_cols_list(field.dataType)]
            else:
                res_list.append(field.name)
        return res_list

    def get_arrays(schema: StructType, depth: int = None) -> list:
        res_list = []
        for field in schema:
            if isinstance(field.dataType, StructType):
                res_list += [".".join([field.name, str(col)]) for col in get_arrays(field.dataType)]
            elif isinstance(field.dataType, ArrayType):
                res_list.append(field.name)
                if not depth:
                    continue
                if isinstance(field.dataType.elementType, StructType):
                    res_list += [".".join([field.name, str(col)]) for col in
                                 get_arrays(field.dataType.elementType, depth - 1)]
        return res_list

    arrays = get_arrays(df.schema)  # [x for x in get_arrays(df.schema) if str(x).count(".") == 1]
    print("arrays", arrays)
    explode_expr = list(map(lambda x: (x, f.explode_outer(x).alias(str(x).replace(".", "_"))), arrays))
    for expr in explode_expr:
        print(expr[0])
        df = df.select("*", expr[1])
        df = df.drop(f.col(expr[0]))

    df.printSchema()

    arrays = set(get_arrays(df.schema)).difference(
        set(arrays))  # [x for x in get_arrays(df.schema) if str(x).count(".") == 1]
    explode_expr = list(map(lambda x: (x, f.explode_outer(x).alias(str(x).replace(".", "_"))), arrays))
    print("set_arrays", arrays)
    for expr in explode_expr:
        print(expr[0])
        df = df.select(f.col("*"), expr[1])
        df = df.drop(f.col(expr[0]))
    df.printSchema()
    cols = get_cols_list(df.schema)
    print("cols", cols)
    df.printSchema()
    struct_expr = list(map(lambda x: f.col(x).alias(str(x).replace(".", "_")), cols))
    for x in struct_expr:
        print(x)
    return df.select(*struct_expr)
