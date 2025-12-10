# Word2Vec and GPT Mapping Implementation

## Word2Vec Column Mapping

### How It Works

Word2Vec maps column names using **semantic word embeddings** to find similar meanings between source and target columns.

### Core Functions

#### 1. `preprocess_column_name(column)`
**Purpose**: Breaks column names into meaningful tokens.

**Process**:
```python
# Input: "userFirstName" or "user_id"
# Step 1: Remove special chars → "userFirstName", "user id"
# Step 2: Split camelCase → ["user", "First", "Name"] or ["user", "id"]
# Step 3: Lowercase → ["user", "first", "name"] or ["user", "id"]
```

**Example**:
- `"customer_email"` → `["customer", "email"]`
- `"OrderTotal"` → `["order", "total"]`

#### 2. `load_word2vec_model(df, extra_df)`
**Purpose**: Load or train Word2Vec model for similarity calculations.

**Process**:
1. **Try loading pre-trained Google News model** (300-dimensional vectors)
   - File: `GoogleNews-vectors-negative300.bin`
   - Contains 3 million words trained on Google News
   
2. **Fallback: Train custom model** if Google News unavailable
   - Uses all column names from both DataFrames
   - Creates 100-dimensional vectors
   - Window size: 5 (considers 5 neighboring words)

#### 3. `calculate_word2vec_similarity(col1, col2, model)`
**Purpose**: Calculate semantic similarity between two column names.

**Process**:
```python
# Input: col1 = "user_email", col2 = "customer_mail"

# Step 1: Preprocess both
words1 = ["user", "email"]        # from col1
words2 = ["customer", "mail"]     # from col2

# Step 2: Get vectors for each word
vec1 = [model["user"], model["email"]]
vec2 = [model["customer"], model["mail"]]

# Step 3: Average the vectors
vec1_avg = mean([vector for "user", vector for "email"])
vec2_avg = mean([vector for "customer", vector for "mail"])

# Step 4: Calculate cosine similarity
similarity = dot(vec1_avg, vec2_avg) / (norm(vec1_avg) * norm(vec2_avg))
# Returns: 0.92 (high similarity)
```

**Why it works**: Word2Vec understands that "user" ≈ "customer" and "email" ≈ "mail" semantically.

#### 4. `word2vec_column_mapping(df, extra_df, missing_cols, extra_cols, mapped_cols, threshold=0.87)`
**Purpose**: Map unmapped columns using Word2Vec similarity.

**Process**:
```python
# Input:
missing_cols = ["customer_email", "order_total"]  # Need these
extra_cols = ["user_mail", "total_price"]         # Have these

# Step 1: Load model
model = load_word2vec_model(df, extra_df)

# Step 2: For each missing column, find best match
for missing_col in missing_cols:
    best_match = None
    best_score = 0.87  # threshold
    
    for extra_col in extra_cols:
        similarity = calculate_word2vec_similarity(missing_col, extra_col, model)
        # "customer_email" vs "user_mail" → 0.91
        # "order_total" vs "total_price" → 0.89
        
        if similarity > best_score:
            best_score = similarity
            best_match = extra_col
    
    if best_match:
        mapped_cols[missing_col] = best_match
        # Result: {"customer_email": "user_mail", "order_total": "total_price"}

# Step 3: Apply mappings
df = df.withColumnRenamed("user_mail", "customer_email")
df = df.withColumnRenamed("total_price", "order_total")
```

**Key Advantage**: Understands semantic relationships (e.g., "price" and "cost", "customer" and "client").

---

## GPT Mapping Functions

### Helper Functions

#### 1. `make_json_safe(data)`
**Purpose**: Convert Python objects to JSON-serializable format for API calls.

**What It Does**:
```python
# Handles these conversions:
datetime.date(2024, 1, 1) → "2024-01-01"
datetime.datetime(...) → "2024-01-01T10:30:00"
{1, 2, 3} → [1, 2, 3]  # set to list
CustomObject() → "CustomObject(...)"  # str representation

# Recursive processing:
data = {
    "date": datetime.now(),
    "items": [{"id": 1, "created": datetime.now()}]
}
# Becomes:
{
    "date": "2024-12-10T17:20:00",
    "items": [{"id": 1, "created": "2024-12-10T17:20:00"}]
}
```

**Why Needed**: Gemini API only accepts JSON-safe data types.

#### 2. `detect_table(df, columns_info, threshold=0.5)`
**Purpose**: Identify which canonical table a DataFrame belongs to based on column overlap.

**Process**:
```python
# Input:
df.columns = ["user_id", "email", "name", "phone"]
columns_info = [
    ("customers", "customer_id", "int"),
    ("customers", "email", "string"),
    ("customers", "name", "string"),
    ("orders", "order_id", "int"),
    ("orders", "user_id", "int")
]

# Step 1: Count matches per table
table_scores = {
    "customers": 2,  # matches: email, name
    "orders": 1      # matches: user_id
}

# Step 2: Calculate overlap ratio
best_table = "customers"  # highest score
overlap_ratio = 2 / 4 = 0.5  # 2 matches out of 4 columns

# Step 3: Check threshold
if 0.5 >= 0.5:  # meets threshold
    return "customers"
```

**Use Case**: In streaming, when DataFrame name is unknown, this detects the target table.

#### 3. `simplify_sample_data(pdf, max_rows=3, max_str_length=100)`
**Purpose**: Truncate sample data to avoid triggering Gemini's safety filters.

**Process**:
```python
# Input DataFrame:
pdf = pd.DataFrame({
    "description": ["Very long product description with 500 characters..."],
    "email": ["user@example.com"]
})

# Output:
{
    "description": ["Very long product description with 500 char..."],  # truncated
    "email": ["user@example.com"]
}
```

**Why Needed**: Long text or certain patterns can trigger safety filters, blocking API responses.

#### 4. `call_gemini_with_retry(prompt, max_retries=3, initial_delay=1)`
**Purpose**: Call Gemini API with exponential backoff and error handling.

**Process**:
```python
# Attempt 1: Try API call
try:
    response = model.generate_content(prompt, safety_settings=...)
    if response.parts:  # Success
        return response.text
    else:  # Blocked by safety filters
        wait 1 second, retry
        
# Attempt 2: Retry with delay
try:
    response = model.generate_content(prompt, safety_settings=...)
    if blocked again:
        wait 2 seconds, retry
        
# Attempt 3: Final attempt
try:
    response = model.generate_content(prompt, safety_settings=...)
    if still blocked:
        raise error
```

**Safety Settings**: Set to `BLOCK_NONE` for all categories to allow data engineering content.

#### 5. `fuzzy_column_mapping(missing_cols, extra_cols)`
**Purpose**: Fallback string matching when Gemini API fails or is blocked.

**Process**:
```python
# Input:
missing_cols = ["customer_name"]
extra_cols = ["customername", "user_email"]

# Calculate string similarity
for missing_col in missing_cols:
    for extra_col in extra_cols:
        # Compare without underscores/hyphens
        "customername" vs "customername" → 1.0 (exact match!)
        "customername" vs "useremail" → 0.3 (low similarity)
    
    if best_score > 0.6:  # threshold
        match "customer_name" → "customername"
```

**Algorithm**: Uses Python's `SequenceMatcher` (Ratcliff/Obershelp algorithm).

### Main Function: `gpt_schema_mapping(df, missing_cols, extra_cols, mapped_cols)`

**Purpose**: Use AI to intelligently map columns based on names, types, and sample data.

**Complete Process**:

```python
# INPUT STATE:
df.columns = ["usr_email", "full_name", "phn_number"]
missing_cols = ["customer_email", "customer_name", "phone"]
extra_cols = ["usr_email", "full_name", "phn_number"]
mapped_cols = {}  # from previous mapping stages

# STEP 1: Prepare data
pdf = df.limit(5).toPandas()  # Get sample rows
simplified_sample = simplify_sample_data(pdf)  # Truncate long strings

schema_info = {
    "missing_cols": ["customer_email", "customer_name", "phone"],
    "extra_cols": ["usr_email", "full_name", "phn_number"],
    "already_mapped": {},
    "sample_data": {"usr_email": ["john@example.com"], ...},
    "data_types": {"usr_email": "object", "full_name": "object", ...}
}
schema_info = make_json_safe(schema_info)  # Convert to JSON-safe

# STEP 2: Build prompt for Gemini
prompt = """
Task: Map source DataFrame columns to target schema columns.

Target schema columns (missing): ["customer_email", "customer_name", "phone"]
Source DataFrame columns (extra): ["usr_email", "full_name", "phn_number"]

Column data types:
{"usr_email": "object", "full_name": "object", "phn_number": "object"}

Sample data (3 rows):
{"usr_email": ["john@example.com"], "full_name": ["John Doe"], ...}

Instructions:
1. Match based on name similarity, data types, and sample content
2. Output ONLY valid JSON
3. Structure:
{
  "mapped_cols": {"customer_email": "usr_email", ...},
  "remaining_missing_cols": [],
  "remaining_extra_cols": []
}
"""

# STEP 3: Call Gemini API with retry logic
try:
    result_text = call_gemini_with_retry(prompt)
    # Response: '{"mapped_cols": {"customer_email": "usr_email", ...}}'
    
    # Clean markdown formatting if present
    if "```json" in result_text:
        result_text = extract_json_from_markdown(result_text)
    
    model_mapping = json.loads(result_text)
    new_mappings = model_mapping["mapped_cols"]
    # Result: {"customer_email": "usr_email", "customer_name": "full_name", "phone": "phn_number"}
    
except (json.JSONDecodeError, Exception) as e:
    # FALLBACK: Use fuzzy string matching
    model_mapping = fuzzy_column_mapping(missing_cols, extra_cols)
    new_mappings = model_mapping["mapped_cols"]

# STEP 4: Apply existing mappings (from previous stages)
for target_col, source_col in mapped_cols.items():
    if source_col in df.columns:
        df = df.withColumnRenamed(source_col, target_col)

# STEP 5: Apply new mappings from Gemini
for target_col, source_col in new_mappings.items():
    if source_col in df.columns:
        df = df.withColumnRenamed(source_col, target_col)
        # "usr_email" → "customer_email"
        # "full_name" → "customer_name"
        # "phn_number" → "phone"

# STEP 6: Update mapping dictionary
mapped_cols.update(new_mappings)

# STEP 7: Drop unmapped extra columns
remaining_extra = ["unknown_col"]  # columns that couldn't be mapped
df = df.drop(*remaining_extra)

# STEP 8: Add missing columns with null values
for col in remaining_missing_cols:
    if col not in df.columns:
        df = df.withColumn(col, lit(None))

# OUTPUT STATE:
df.columns = ["customer_email", "customer_name", "phone"]
```

### Why GPT Mapping is Powerful

**1. Context-Aware**: Considers multiple factors
- Column names (e.g., "usr_email" → "customer_email")
- Data types (e.g., both are strings)
- Sample data (e.g., contains "@" indicating email)

**2. Handles Variations**:
- Abbreviations: "usr" → "user", "phn" → "phone"
- Different naming conventions: "snake_case" vs "camelCase"
- Synonyms: "user" vs "customer", "phone" vs "mobile"

**3. Intelligent Decisions**:
```python
# Example: Multiple possible matches
extra_cols = ["email", "user_mail", "contact_email"]
missing_cols = ["customer_email"]

# GPT analyzes sample data:
# "email" contains: ["john@example.com", "jane@example.com"]
# "user_mail" contains: [null, null]
# "contact_email" contains: ["support@company.com"]

# Decision: Map "customer_email" ← "email" 
# Reason: More populated, better semantic match
```

**4. Fallback Strategy**:
- **First**: Try Gemini AI (most intelligent)
- **Second**: Retry with exponential backoff (handles temporary failures)
- **Third**: Use fuzzy string matching (handles API blocks)
- **Result**: Always produces a mapping, never fails completely

---

## Integration in Pipeline

Both Word2Vec and GPT mapping are part of an 8-stage sequential pipeline in `map.py`:

```
1. Predefined variants → 2. RapidFuzz → 3. NLTK → 4. WordNet → 
5. spaCy → 6. Word2Vec → 7. RoBERTa → 8. GPT (final fallback)
```

Each stage processes only **unmapped columns** from the previous stage, becoming progressively more sophisticated but also more expensive computationally.

**Word2Vec Position**: Stage 6 (after simpler string matching, before transformer models)
**GPT Position**: Stage 8 (final fallback with highest intelligence but highest cost)

---

## Summary

### Word2Vec
- **Method**: Semantic word embeddings
- **Strength**: Understands word meanings and relationships
- **Cost**: Low (once model loaded)
- **Example**: "customer_email" ≈ "user_mail" (similarity: 0.91)

### GPT Mapping
- **Method**: AI-powered multi-factor analysis
- **Strength**: Context-aware, handles complex cases
- **Cost**: High (API calls)
- **Example**: Maps "usr_email" → "customer_email" by analyzing name + type + sample data

### Helper Functions
- `make_json_safe()`: Converts Python objects to JSON for API
- `detect_table()`: Identifies target table from column overlap
- `simplify_sample_data()`: Truncates data to avoid safety filters
- `call_gemini_with_retry()`: Handles API calls with exponential backoff
- `fuzzy_column_mapping()`: String-based fallback when AI fails
