import json
import requests
from pyspark.sql.functions import lit
from utils.helpers import make_json_safe

FREEGPT4_API_URL = (
    "http://localhost:5500/v1/chat/completions"  # adjust if hosted elsewhere
)


def gpt_schema_mapping(df, missing_cols, extra_cols, mapped_cols):
    """
    Map columns using GPT language model.

    Args:
        df: Spark DataFrame
        missing_cols: List of missing columns
        extra_cols: List of extra columns
        mapped_cols: Dictionary of mapped columns

    Returns:
        Tuple of (df, missing_cols, extra_cols, mapped_cols)
    """
    pdf = df.limit(5).toPandas()

    schema_info = {
        "missing_cols": missing_cols,
        "extra_cols": extra_cols,
        "already_mapped": mapped_cols,
        "sample_data": pdf.to_dict(orient="list"),
    }

    # Ensure schema_info is JSON safe
    schema_info = make_json_safe(schema_info)

    prompt = f"""
        You are a data engineer helping with schema alignment.

        I have a dataset with extra columns and some missing schema columns.
        Please map extra columns to missing schema columns based on semantics and sample data.

        Input (JSON):
        {json.dumps(schema_info, indent=2)}

        Return ONLY valid JSON with this exact structure:
        {{
        "mapped_cols": {{"schema_col": "df_col", ...}},
        "remaining_missing_cols": [],
        "remaining_extra_cols": []
        }}
    """

    # Call FreeGPT4-WEB-API instead of OpenAI
    response = requests.post(
        FREEGPT4_API_URL,
        json={
            "model": "gpt-4.0",  # adjust model name based on what FreeGPT4 API supports
            "messages": [{"role": "user", "content": prompt}],
            "temperature": 0,
        },
    )

    print("Full response:", response.json())

    try:
        result_text = response.json()["choices"][0]["message"]["content"].strip()
        model_mapping = json.loads(result_text)

        mapped_cols.update(model_mapping["mapped_cols"])
        missing_cols = model_mapping["remaining_missing_cols"]
        extra_cols = model_mapping["remaining_extra_cols"]

        # Rename + drop + add cols in df
        for schema_col, df_col in model_mapping["mapped_cols"].items():
            df = df.withColumnRenamed(df_col, schema_col)
        df = df.drop(*extra_cols)
        for col in missing_cols:
            df = df.withColumn(col, lit(None))

    except Exception as e:
        print("Error parsing model output:", e)
        model_mapping = {}

    return df, missing_cols, extra_cols, mapped_cols
