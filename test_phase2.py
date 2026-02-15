#!/usr/bin/env python3
"""
Test script for Phase 2 Spark Structured Streaming implementation.

This script validates the streaming pipeline components without requiring
a full Spark cluster running.
"""

import os
import sys

def test_imports():
    """Test that streaming modules can be imported."""
    print("=" * 60)
    print("Testing Module Imports")
    print("=" * 60)
    
    try:
        sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'cleaning'))
        sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'transformation'))
        
        print("\n✓ Test 1: Import streaming_cleaning")
        import streaming_cleaning
        print("  ✅ streaming_cleaning imported successfully")
        print(f"  Classes: {[c for c in dir(streaming_cleaning) if c.startswith('Streaming')]}")
        
        print("\n✓ Test 2: Import streaming_transformation")
        import streaming_transformation
        print("  ✅ streaming_transformation imported successfully")
        print(f"  Classes: {[c for c in dir(streaming_transformation) if c.startswith('Streaming')]}")
        
        print("\n✓ Test 3: Import streaming_orchestrator")
        sys.path.insert(0, os.path.dirname(__file__))
        import streaming_orchestrator
        print("  ✅ streaming_orchestrator imported successfully")
        print(f"  Classes: {[c for c in dir(streaming_orchestrator) if c.startswith('Streaming')]}")
        
        return True
        
    except ImportError as e:
        print(f"\n❌ Error importing modules: {e}")
        return False
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_class_structure():
    """Test that classes have expected methods."""
    print("\n" + "=" * 60)
    print("Testing Class Structure")
    print("=" * 60)
    
    try:
        from cleaning.streaming_cleaning import StreamingCleaner
        from transformation.streaming_transformation import StreamingTransformer
        from streaming_orchestrator import StreamingOrchestrator
        
        print("\n✓ Test 4: StreamingCleaner structure")
        expected_methods = [
            'create_input_stream',
            'apply_cleaning_rules',
            'write_stream',
            'create_cleaning_pipeline'
        ]
        for method in expected_methods:
            if hasattr(StreamingCleaner, method):
                print(f"  ✅ {method} exists")
            else:
                print(f"  ❌ {method} missing")
                return False
        
        print("\n✓ Test 5: StreamingTransformer structure")
        expected_methods = [
            'create_input_stream',
            'aggregate_orders_streaming',
            'write_stream',
            'create_transformation_pipeline'
        ]
        for method in expected_methods:
            if hasattr(StreamingTransformer, method):
                print(f"  ✅ {method} exists")
            else:
                print(f"  ❌ {method} missing")
                return False
        
        print("\n✓ Test 6: StreamingOrchestrator structure")
        expected_methods = [
            'initialize_spark',
            'start_cleaning_pipeline',
            'start_transformation_pipeline',
            'monitor_queries',
            'stop_all_queries'
        ]
        for method in expected_methods:
            if hasattr(StreamingOrchestrator, method):
                print(f"  ✅ {method} exists")
            else:
                print(f"  ❌ {method} missing")
                return False
        
        return True
        
    except Exception as e:
        print(f"\n❌ Error testing class structure: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_spark_dependency():
    """Test that PySpark is available."""
    print("\n" + "=" * 60)
    print("Testing Spark Dependency")
    print("=" * 60)
    
    try:
        import pyspark
        print(f"\n✅ PySpark installed: version {pyspark.__version__}")
        
        from pyspark.sql import SparkSession
        print("✅ SparkSession available")
        
        from pyspark.sql.functions import col, window, count
        print("✅ Streaming functions available")
        
        return True
        
    except ImportError as e:
        print(f"\n❌ PySpark not installed: {e}")
        print("  Install with: pip install pyspark==3.5.0")
        return False
    except Exception as e:
        print(f"\n❌ Error checking PySpark: {e}")
        return False


def test_checkpoint_directory():
    """Test checkpoint directory setup."""
    print("\n" + "=" * 60)
    print("Testing Checkpoint Directory")
    print("=" * 60)
    
    checkpoint_base = "/tmp/spark_checkpoints"
    
    try:
        if not os.path.exists(checkpoint_base):
            os.makedirs(checkpoint_base)
            print(f"\n✅ Created checkpoint directory: {checkpoint_base}")
        else:
            print(f"\n✅ Checkpoint directory exists: {checkpoint_base}")
        
        # Test write permissions
        test_file = os.path.join(checkpoint_base, "test_write")
        with open(test_file, 'w') as f:
            f.write("test")
        os.remove(test_file)
        print("✅ Write permissions OK")
        
        return True
        
    except Exception as e:
        print(f"\n❌ Error with checkpoint directory: {e}")
        return False


def test_configuration():
    """Test configuration values."""
    print("\n" + "=" * 60)
    print("Testing Configuration")
    print("=" * 60)
    
    try:
        # Check environment variables
        required_vars = [
            'MINIO_ENDPOINT',
            'MINIO_ACCESS_KEY',
            'MINIO_SECRET_KEY'
        ]
        
        all_ok = True
        for var in required_vars:
            value = os.getenv(var)
            if value:
                display_value = '***' if 'KEY' in var or 'SECRET' in var else value
                print(f"✅ {var} = {display_value}")
            else:
                print(f"⚠️  {var} is NOT set (optional for testing)")
        
        print("\n✅ Configuration check complete")
        return True
        
    except Exception as e:
        print(f"\n❌ Error checking configuration: {e}")
        return False


def print_usage_examples():
    """Print usage examples."""
    print("\n" + "=" * 60)
    print("Usage Examples")
    print("=" * 60)
    
    print("\n📚 Run Streaming Pipeline:")
    print("=" * 60)
    
    examples = [
        ("Complete pipeline", "python streaming_orchestrator.py"),
        ("Cleaning only", "python streaming_orchestrator.py --cleaning-only"),
        ("Transformation only", "python streaming_orchestrator.py --transformation-only"),
        ("Custom interval", "python streaming_orchestrator.py --trigger-interval '5 seconds'"),
        ("Individual cleaning", "python cleaning/streaming_cleaning.py"),
        ("Individual transform", "python transformation/streaming_transformation.py"),
    ]
    
    for desc, cmd in examples:
        print(f"\n{desc}:")
        print(f"  {cmd}")
    
    print("\n" + "=" * 60)


def main():
    """Run all tests."""
    print("\n" + "🧪" * 30)
    print("PHASE 2: SPARK STRUCTURED STREAMING - TEST SUITE")
    print("🧪" * 30 + "\n")
    
    tests = [
        ("Module Imports", test_imports),
        ("Class Structure", test_class_structure),
        ("Spark Dependency", test_spark_dependency),
        ("Checkpoint Directory", test_checkpoint_directory),
        ("Configuration", test_configuration),
    ]
    
    results = []
    for test_name, test_func in tests:
        try:
            result = test_func()
            results.append((test_name, result))
        except Exception as e:
            print(f"\n❌ {test_name} failed with exception: {e}")
            results.append((test_name, False))
    
    # Summary
    print("\n" + "=" * 60)
    print("TEST SUMMARY")
    print("=" * 60)
    
    for test_name, result in results:
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"{test_name}: {status}")
    
    passed = sum(1 for _, result in results if result)
    total = len(results)
    
    print(f"\nTotal: {passed}/{total} tests passed")
    
    if passed == total:
        print("\n✅ Phase 2 implementation is ready!")
        print_usage_examples()
        
        print("\n🎯 Next Steps:")
        print("1. Ensure MinIO is running with data in mapped/ folder")
        print("2. Run: python streaming_orchestrator.py")
        print("3. Monitor logs for processing status")
        print("4. Verify output in cleaned_streaming/ and transformed_streaming/")
        print("5. Press Ctrl+C to stop gracefully")
        
        return 0
    else:
        print("\n❌ Some tests failed. Fix issues before proceeding.")
        print("\n💡 Common fixes:")
        print("- Install PySpark: pip install pyspark==3.5.0")
        print("- Check file paths and imports")
        print("- Verify checkpoint directory permissions")
        
        return 1


if __name__ == "__main__":
    sys.exit(main())
