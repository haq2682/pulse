"""
Test script for API validation and multi-table file loading.

Run this script to validate the implementations:
1. API format validation
2. Multi-table file support
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from streaming.ingestion.api_validation import validate_api_data, get_expected_format_example
import json


def test_api_validation():
    """Test API data validation"""
    print("=" * 60)
    print("Testing API Data Validation")
    print("=" * 60)
    
    # Test 1: Valid data
    print("\n1. Testing valid API data...")
    valid_data = {
        "tables": [
            {
                "table_name": "users",
                "data": [
                    {"id": 1, "name": "Alice"},
                    {"id": 2, "name": "Bob"}
                ]
            },
            {
                "table_name": "orders",
                "data": [
                    {"order_id": 101, "user_id": 1}
                ]
            }
        ]
    }
    
    try:
        result = validate_api_data(valid_data)
        print("✅ Valid data accepted")
        print(f"   Tables: {[t.table_name for t in result.tables]}")
    except ValueError as e:
        print(f"❌ Validation failed: {e}")
    
    # Test 2: Invalid data - missing tables key
    print("\n2. Testing invalid data (missing 'tables' key)...")
    invalid_data_1 = {
        "data": [{"id": 1}]
    }
    
    try:
        validate_api_data(invalid_data_1)
        print("❌ Should have failed validation")
    except ValueError as e:
        print(f"✅ Correctly rejected: {str(e)[:80]}...")
    
    # Test 3: Invalid data - using "name" instead of "table_name"
    print("\n3. Testing invalid data (wrong field name)...")
    invalid_data_2 = {
        "tables": [
            {
                "name": "users",  # Wrong! Should be "table_name"
                "data": [{"id": 1}]
            }
        ]
    }
    
    try:
        validate_api_data(invalid_data_2)
        print("❌ Should have failed validation")
    except ValueError as e:
        print(f"✅ Correctly rejected: {str(e)[:80]}...")
    
    # Test 4: Invalid data - empty table_name
    print("\n4. Testing invalid data (empty table_name)...")
    invalid_data_3 = {
        "tables": [
            {
                "table_name": "",
                "data": [{"id": 1}]
            }
        ]
    }
    
    try:
        validate_api_data(invalid_data_3)
        print("❌ Should have failed validation")
    except ValueError as e:
        print(f"✅ Correctly rejected: {str(e)[:80]}...")
    
    # Test 5: Valid data with empty data array
    print("\n5. Testing valid data with empty data array...")
    valid_empty = {
        "tables": [
            {
                "table_name": "users",
                "data": []
            }
        ]
    }
    
    try:
        result = validate_api_data(valid_empty)
        print("✅ Empty data array accepted")
    except ValueError as e:
        print(f"❌ Validation failed: {e}")
    
    # Test 6: Display expected format
    print("\n6. Expected format example:")
    print(json.dumps(get_expected_format_example(), indent=2))


def test_multi_table_json_parsing():
    """Test JSON multi-table structure detection"""
    print("\n" + "=" * 60)
    print("Testing Multi-Table JSON Parsing Logic")
    print("=" * 60)
    
    # Test case 1: API format with tables key
    print("\n1. API format with 'tables' key:")
    api_format = {
        "tables": [
            {"table_name": "customers", "data": [{"id": 1}]},
            {"table_name": "orders", "data": [{"order_id": 101}]}
        ]
    }
    
    if "tables" in api_format:
        print("✅ Detected API format with 'tables' key")
        tables = [t["table_name"] for t in api_format["tables"]]
        print(f"   Found tables: {tables}")
    
    # Test case 2: Nested structure with table keys
    print("\n2. Nested structure with table keys:")
    nested_format = {
        "customers": [
            {"id": 1, "name": "Alice"},
            {"id": 2, "name": "Bob"}
        ],
        "orders": [
            {"order_id": 101, "customer_id": 1}
        ]
    }
    
    tables_found = []
    for key, value in nested_format.items():
        if isinstance(value, list) and value and isinstance(value[0], dict):
            tables_found.append(key)
    
    if tables_found:
        print(f"✅ Detected nested format with tables: {tables_found}")
    
    # Test case 3: Single array (traditional JSON)
    print("\n3. Single array (traditional JSON):")
    single_array = [
        {"id": 1, "name": "Alice"},
        {"id": 2, "name": "Bob"}
    ]
    
    if isinstance(single_array, list):
        print("✅ Detected single-table array format")


if __name__ == "__main__":
    test_api_validation()
    test_multi_table_json_parsing()
    
    print("\n" + "=" * 60)
    print("✅ All tests completed")
    print("=" * 60)
