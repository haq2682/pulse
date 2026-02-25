import json
import time
import os
from google import genai
from google.genai import types
from google.genai.errors import APIError
from pyspark.sql.functions import lit
from dotenv import load_dotenv, find_dotenv
from utils.helpers import make_json_safe

load_dotenv(find_dotenv())

# Single client instance reused across all calls
client = genai.Client(api_key=os.getenv("GEMINI_API_KEY"))

# Build config once — safety settings disable all filters for data engineering tasks
_GENERATE_CONFIG = types.GenerateContentConfig(
    temperature=0.0,
    top_p=0.95,
    top_k=40,
    max_output_tokens=2048,
    safety_settings=[
        types.SafetySetting(
            category=types.HarmCategory.HARM_CATEGORY_HATE_SPEECH,
            threshold=types.HarmBlockThreshold.BLOCK_NONE,
        ),
        types.SafetySetting(
            category=types.HarmCategory.HARM_CATEGORY_HARASSMENT,
            threshold=types.HarmBlockThreshold.BLOCK_NONE,
        ),
        types.SafetySetting(
            category=types.HarmCategory.HARM_CATEGORY_SEXUALLY_EXPLICIT,
            threshold=types.HarmBlockThreshold.BLOCK_NONE,
        ),
        types.SafetySetting(
            category=types.HarmCategory.HARM_CATEGORY_DANGEROUS_CONTENT,
            threshold=types.HarmBlockThreshold.BLOCK_NONE,
        ),
    ],
)


def simplify_sample_data(pdf, max_rows=3, max_str_length=100):
    """
    Simplify sample data to reduce safety filter triggers.
    Truncates long strings and limits rows.
    """
    sample_dict = pdf.head(max_rows).to_dict(orient="list")

    for key, values in sample_dict.items():
        sample_dict[key] = [
            (
                str(v)[:max_str_length] + "..."
                if isinstance(v, str) and len(str(v)) > max_str_length
                else v
            )
            for v in values
        ]

    return sample_dict


def _is_response_blocked(response) -> bool:
    """
    Return True if the response was blocked by safety filters or has no usable content.
    In the new SDK, accessing response.text on a blocked response raises ValueError,
    so we inspect candidates directly instead.
    """
    if not response.candidates:
        return True
    candidate = response.candidates[0]
    # finish_reason == "STOP" (or enum value 1) means a clean completion
    finish_reason = candidate.finish_reason
    # Accept both the string name and the enum integer value
    if hasattr(finish_reason, "name"):
        return finish_reason.name not in ("STOP", "MAX_TOKENS")
    return str(finish_reason) not in ("STOP", "MAX_TOKENS", "1", "2")


def call_gemini_with_retry(prompt, max_retries=3, initial_delay=1):
    """Call Gemini API with exponential backoff retry logic and safety settings."""

    for attempt in range(max_retries):
        try:
            response = client.models.generate_content(
                model="gemini-2.5-flash",
                contents=prompt,
                config=_GENERATE_CONFIG,
            )

            # Check if response was blocked by safety filters
            if _is_response_blocked(response):
                print("⚠ Response blocked by safety filters")

                if response.prompt_feedback:
                    print(f"  Prompt feedback: {response.prompt_feedback}")

                if response.candidates:
                    candidate = response.candidates[0]
                    print(f"  Finish reason: {candidate.finish_reason}")
                    if candidate.safety_ratings:
                        print("  Safety ratings:")
                        for rating in candidate.safety_ratings:
                            print(f"    - {rating.category}: {rating.probability}")

                if attempt < max_retries - 1:
                    delay = initial_delay * (2 ** attempt)
                    print(f"  Retrying after {delay}s...")
                    time.sleep(delay)
                    continue
                else:
                    raise ValueError(
                        f"Response blocked by safety filters after {max_retries} attempts. "
                        "Try simplifying your data or using fallback mapping."
                    )

            # Safely extract text — raises ValueError if no text part exists
            try:
                return response.text
            except ValueError as text_err:
                raise ValueError(
                    f"Response returned no text content: {text_err}"
                ) from text_err

        except APIError as e:
            # API-level errors (auth, quota, network, etc.)
            if attempt == max_retries - 1:
                raise
            delay = initial_delay * (2 ** attempt)
            print(f"  Retry {attempt + 1}/{max_retries} after {delay}s — APIError: {e}")
            time.sleep(delay)

        except ValueError:
            # Re-raise ValueErrors (blocked content, no text) immediately — retrying won't help
            raise

        except Exception as e:
            if attempt == max_retries - 1:
                raise
            delay = initial_delay * (2 ** attempt)
            print(f"  Retry {attempt + 1}/{max_retries} after {delay}s due to: {e}")
            time.sleep(delay)


def fuzzy_column_mapping(missing_cols, extra_cols):
    """
    Fallback fuzzy matching when AI is blocked or fails.
    Uses string similarity to match column names.
    """
    from difflib import SequenceMatcher

    mappings = {}
    remaining_missing = missing_cols.copy()
    remaining_extra = extra_cols.copy()

    print(
        f"  Starting fuzzy matching for {len(missing_cols)} missing cols "
        f"and {len(extra_cols)} extra cols"
    )

    for missing_col in missing_cols:
        best_match = None
        best_score = 0.6  # Minimum similarity threshold

        for extra_col in extra_cols:
            score = SequenceMatcher(
                None,
                missing_col.lower().replace("_", "").replace("-", ""),
                extra_col.lower().replace("_", "").replace("-", ""),
            ).ratio()

            if score > best_score:
                best_score = score
                best_match = extra_col

        if best_match:
            mappings[missing_col] = best_match
            if best_match in remaining_extra:
                remaining_extra.remove(best_match)
            if missing_col in remaining_missing:
                remaining_missing.remove(missing_col)
            print(
                f"  Fuzzy matched: '{best_match}' -> '{missing_col}' (score: {best_score:.2f})"
            )
        else:
            print(f"  No match found for: '{missing_col}'")

    return {
        "mapped_cols": mappings,
        "remaining_missing_cols": remaining_missing,
        "remaining_extra_cols": remaining_extra,
    }


def _apply_fallback_fuzzy(df, missing_cols, extra_cols):
    """Helper to run fuzzy matching, filtering extra_cols to only existing DataFrame columns."""
    current_df_cols = df.columns
    available_extra_cols = [col for col in extra_cols if col in current_df_cols]
    return fuzzy_column_mapping(missing_cols, available_extra_cols)


def gpt_schema_mapping(df, missing_cols, extra_cols, mapped_cols):
    """
    Map columns using Google Gemini Flash language model with retry logic.

    Args:
        df: Spark DataFrame
        missing_cols: List of missing columns
        extra_cols: List of extra columns
        mapped_cols: Dictionary of mapped columns (target_col: source_col)

    Returns:
        Tuple of (df, missing_cols, extra_cols, mapped_cols)
    """

    print(f"\n🔍 DEBUG - Input to gpt_schema_mapping:")
    print(f"  DataFrame columns: {df.columns}")
    print(f"  Missing cols: {missing_cols}")
    print(f"  Extra cols: {extra_cols}")
    print(f"  Mapped cols (should be renamed): {mapped_cols}")
    print()

    pdf = df.limit(5).toPandas()

    simplified_sample = simplify_sample_data(pdf, max_rows=3, max_str_length=100)

    schema_info = {
        "missing_cols": missing_cols,
        "extra_cols": extra_cols,
        "already_mapped": mapped_cols,
        "sample_data": simplified_sample,
        "data_types": {col: str(dtype) for col, dtype in pdf.dtypes.items()},
    }

    schema_info = make_json_safe(schema_info)

    prompt = f"""
Task: Map source DataFrame columns to target schema columns.

Target schema columns (missing): {json.dumps(missing_cols)}
Source DataFrame columns (extra): {json.dumps(extra_cols)}
Already mapped: {json.dumps(mapped_cols)}

Column data types:
{json.dumps(schema_info['data_types'], indent=2)}

Sample data (3 rows):
{json.dumps(simplified_sample, indent=2)}

Instructions:
1. Match source columns to target columns based on:
   - Name similarity (e.g., "user_name" matches "username")
   - Data type compatibility
   - Sample data content
2. Output ONLY valid JSON (no markdown, no explanations)
3. Use this exact structure:

{{
  "mapped_cols": {{"target_column": "source_column"}},
  "remaining_missing_cols": ["unmapped_target1"],
  "remaining_extra_cols": ["unmapped_source1"]
}}
"""

    new_mappings = {}
    updated_missing_cols = missing_cols.copy()
    updated_extra_cols = extra_cols.copy()

    try:
        result_text = call_gemini_with_retry(prompt)
        print(
            "Full response:",
            result_text[:300] + ("..." if len(result_text) > 300 else ""),
        )

        result_text = result_text.strip()

        # Strip markdown code fences if present
        if "```json" in result_text:
            result_text = result_text.split("```json")[1].split("```")[0].strip()
        elif "```" in result_text:
            parts = result_text.split("```")
            if len(parts) >= 3:
                result_text = parts[1].strip()

        # Extract the JSON object boundaries
        start_idx = result_text.find("{")
        end_idx = result_text.rfind("}")
        if start_idx != -1 and end_idx != -1:
            result_text = result_text[start_idx: end_idx + 1]

        model_mapping = json.loads(result_text)

        required_keys = {"mapped_cols", "remaining_missing_cols", "remaining_extra_cols"}
        if not all(key in model_mapping for key in required_keys):
            raise ValueError(f"Response missing required keys: {required_keys}")

        new_mappings = model_mapping["mapped_cols"]
        updated_missing_cols = model_mapping["remaining_missing_cols"]
        updated_extra_cols = model_mapping["remaining_extra_cols"]

        print(f"✓ Mapped columns: {new_mappings}")
        print(f"✓ Remaining missing: {updated_missing_cols}")
        print(f"✓ Remaining extra: {updated_extra_cols}")

    except json.JSONDecodeError as e:
        print(f"✗ JSON parsing error: {e}")
        if "result_text" in locals():
            print(f"  Raw response: {result_text[:500]}")
        print("⚠ Falling back to fuzzy column matching...")
        model_mapping = _apply_fallback_fuzzy(df, missing_cols, extra_cols)
        new_mappings = model_mapping["mapped_cols"]
        updated_missing_cols = model_mapping["remaining_missing_cols"]
        updated_extra_cols = model_mapping["remaining_extra_cols"]

    except Exception as e:
        print(f"✗ Gemini API error: {e}")
        print("⚠ Falling back to fuzzy column matching...")
        model_mapping = _apply_fallback_fuzzy(df, missing_cols, extra_cols)
        new_mappings = model_mapping["mapped_cols"]
        updated_missing_cols = model_mapping["remaining_missing_cols"]
        updated_extra_cols = model_mapping["remaining_extra_cols"]

    # Apply all column transformations
    print(f"\n🔧 Applying column transformations:")
    print(f"  New mappings from this iteration: {new_mappings}")
    print(f"  Existing mappings to apply: {mapped_cols}")

    # First apply any previously mapped columns that haven't been renamed yet
    for target_col, source_col in mapped_cols.items():
        if source_col in df.columns and target_col not in df.columns:
            df = df.withColumnRenamed(source_col, target_col)
            print(f"  ✓ Renamed (from mapped_cols): '{source_col}' -> '{target_col}'")
        elif source_col not in df.columns and target_col in df.columns:
            print(f"  ℹ Already renamed: '{source_col}' -> '{target_col}'")
        elif source_col not in df.columns and target_col not in df.columns:
            print(f"  ⚠ Warning: Neither '{source_col}' nor '{target_col}' found in DataFrame")

    # Then apply new mappings from this iteration
    for target_col, source_col in new_mappings.items():
        if source_col in df.columns and target_col not in df.columns:
            df = df.withColumnRenamed(source_col, target_col)
            print(f"  ✓ Renamed (new): '{source_col}' -> '{target_col}'")
        elif target_col in df.columns:
            print(f"  ℹ Column '{target_col}' already exists, skipping rename from '{source_col}'")
        else:
            print(f"  ⚠ Warning: Column '{source_col}' not found in DataFrame")

    mapped_cols.update(new_mappings)

    # Drop unmapped extra columns that still exist in the DataFrame
    cols_to_drop = [col for col in updated_extra_cols if col in df.columns]
    if cols_to_drop:
        df = df.drop(*cols_to_drop)
        print(f"  ✓ Dropped {len(cols_to_drop)} unmapped columns: {cols_to_drop}")

    # Add missing columns with null values
    for col in updated_missing_cols:
        if col not in df.columns:
            df = df.withColumn(col, lit(None))
            print(f"  ✓ Added missing column: '{col}' (null values)")

    print(f"\n📊 Final DataFrame columns: {df.columns}\n")

    return df, updated_missing_cols, updated_extra_cols, mapped_cols