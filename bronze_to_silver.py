# Для очищення текстових колонок варто використати функцію
import re

def clean_text(text):
    return re.sub(r'[^a-zA-Z0-9,.\\"\']', '', str(text))


# Цю python-функцію необхідно загорнути у spark user defined function і використати:
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

clean_text_udf = udf(clean_text, StringType())

# тут df - це spark DataFrame
df = df.withColumn(col_name, clean_text_udf(df[col_name]))
