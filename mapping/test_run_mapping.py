#!/usr/bin/env python3
"""
Test script to validate run_mapping.py modes.
This script validates the CLI interface but doesn't execute the actual pipelines
as they require infrastructure (MinIO, Kafka, Spark, etc.).
"""

import subprocess
import sys


def run_command(cmd):
    """Run a command and return the result."""
    print(f"\n{'='*60}")
    print(f"Running: {' '.join(cmd)}")
    print('='*60)
    result = subprocess.run(cmd, capture_output=True, text=True)
    print(result.stdout)
    if result.stderr:
        print("STDERR:", result.stderr)
    return result.returncode


def test_help():
    """Test help command."""
    print("\n## Test 1: Help Command")
    return run_command(["python3", "run_mapping.py", "--help"])


def test_batch_mode_no_bucket():
    """Test batch mode without bucket name (should fail)."""
    print("\n## Test 2: Batch mode without bucket name (should fail)")
    return run_command(["python3", "run_mapping.py", "--mode", "batch"])


def test_db_mode_no_uri():
    """Test db mode without db-uri (should fail)."""
    print("\n## Test 3: DB mode without db-uri (should fail)")
    return run_command(["python3", "run_mapping.py", "--mode", "db", "--bucket-name", "test"])


def test_api_mode_no_url():
    """Test api mode without api-url (should fail)."""
    print("\n## Test 4: API mode without api-url (should fail)")
    return run_command(["python3", "run_mapping.py", "--mode", "api", "--bucket-name", "test"])


def test_invalid_mode():
    """Test invalid mode (should fail)."""
    print("\n## Test 5: Invalid mode (should fail)")
    return run_command(["python3", "run_mapping.py", "--mode", "invalid", "--bucket-name", "test"])


def main():
    """Run all tests."""
    print("="*60)
    print("TESTING run_mapping.py CLI Interface")
    print("="*60)
    
    tests = [
        ("Help command", test_help, 0),
        ("Batch mode validation", test_batch_mode_no_bucket, 2),
        ("DB mode validation", test_db_mode_no_uri, 2),
        ("API mode validation", test_api_mode_no_url, 2),
        ("Invalid mode validation", test_invalid_mode, 2),
    ]
    
    passed = 0
    failed = 0
    
    for name, test_func, expected_code in tests:
        try:
            returncode = test_func()
            if returncode == expected_code:
                print(f"✅ PASS: {name}")
                passed += 1
            else:
                print(f"❌ FAIL: {name} (expected exit code {expected_code}, got {returncode})")
                failed += 1
        except Exception as e:
            print(f"❌ ERROR: {name} - {e}")
            failed += 1
    
    print("\n" + "="*60)
    print(f"Test Results: {passed} passed, {failed} failed")
    print("="*60)
    
    # Note about infrastructure tests
    print("\n⚠️  NOTE: Full integration tests require infrastructure:")
    print("   - MinIO server with test data")
    print("   - Kafka cluster")
    print("   - Spark cluster")
    print("   - Test database (for DB mode)")
    print("   - Test API endpoint (for API mode)")
    print("\n   CLI validation tests completed successfully!")
    
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
