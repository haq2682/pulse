"""
Verification script for pipeline service fixes.

This script verifies:
1. Database column name corrections (progress -> progress_percentage)
2. Project root path detection
"""

import sys
import os
import re

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

def verify_column_names():
    """Verify that all database operations use progress_percentage"""
    print("=" * 60)
    print("Verifying Database Column Names")
    print("=" * 60)
    
    # Get the correct path
    script_dir = os.path.dirname(os.path.abspath(__file__))
    pipeline_service_path = os.path.join(script_dir, 'pipeline_service.py')
    
    if not os.path.exists(pipeline_service_path):
        print(f"❌ Could not find pipeline_service.py at {pipeline_service_path}")
        return False
    
    with open(pipeline_service_path, 'r') as f:
        content = f.read()
    
    # Check for INSERT statement
    insert_pattern = r'INSERT INTO pipeline_status.*?VALUES.*?:progress_percentage'
    if re.search(insert_pattern, content, re.DOTALL):
        print("✅ INSERT statement uses :progress_percentage")
    else:
        print("❌ INSERT statement does NOT use :progress_percentage")
        return False
    
    # Check for UPDATE in _update_progress
    update_pattern = r'"progress_percentage":\s*min\(progress,\s*100\)'
    if re.search(update_pattern, content):
        print("✅ UPDATE data uses 'progress_percentage' key")
    else:
        print("❌ UPDATE data does NOT use 'progress_percentage' key")
        return False
    
    # Check that WebSocket messages still use "progress" (not progress_percentage)
    websocket_pattern = r'_broadcast_progress.*?"progress":\s*progress'
    if re.search(websocket_pattern, content, re.DOTALL):
        print("✅ WebSocket messages correctly use 'progress' (not progress_percentage)")
    else:
        print("❌ WebSocket messages may have been incorrectly changed")
        return False
    
    # Check that old "progress" key is not used in database operations
    bad_patterns = [
        r'VALUES.*?:progress[,\)]',  # :progress in VALUES
        r'"progress":\s*min\(progress,\s*100\)',  # "progress" in update_data
    ]
    
    for i, pattern in enumerate(bad_patterns, 1):
        if re.search(pattern, content):
            print(f"❌ Found incorrect 'progress' usage in database operation (pattern {i})")
            return False
    
    print("✅ No incorrect 'progress' usage found in database operations")
    return True

def verify_project_root():
    """Verify project root path detection logic"""
    print("\n" + "=" * 60)
    print("Verifying Project Root Path Detection")
    print("=" * 60)
    
    # Get the correct path
    script_dir = os.path.dirname(os.path.abspath(__file__))
    pipeline_service_path = os.path.join(script_dir, 'pipeline_service.py')
    
    if not os.path.exists(pipeline_service_path):
        print(f"❌ Could not find pipeline_service.py at {pipeline_service_path}")
        return False
    
    with open(pipeline_service_path, 'r') as f:
        content = f.read()
    
    # Check for improved path detection logic
    checks = [
        (r"if '/api/services/' in current_file", "✅ Checks for api/services structure"),
        (r"if '/services/' in current_file", "✅ Checks for services structure"),
        (r"Fallback: assume we're in the project root", "✅ Has fallback logic"),
        (r'print\(f"PipelineService initialized with project_root:', "✅ Logs project_root on init"),
    ]
    
    all_passed = True
    for pattern, message in checks:
        if re.search(pattern, content):
            print(message)
        else:
            print(f"❌ Missing: {message}")
            all_passed = False
    
    return all_passed

def simulate_path_detection():
    """Simulate path detection for different scenarios"""
    print("\n" + "=" * 60)
    print("Simulating Path Detection")
    print("=" * 60)
    
    test_cases = [
        ("/home/user/pulse/api/services/pipeline_service.py", "/home/user/pulse"),
        ("/app/services/pipeline_service.py", "/app"),
        ("/app/api/services/pipeline_service.py", "/app"),
    ]
    
    for current_file, expected_root in test_cases:
        # Simulate the logic
        if '/api/services/' in current_file or current_file.endswith('/api/services/pipeline_service.py'):
            # Development structure: go up 3 levels
            calculated_root = os.path.dirname(os.path.dirname(os.path.dirname(current_file)))
        elif '/services/' in current_file or current_file.endswith('/services/pipeline_service.py'):
            # Container structure: go up 2 levels
            calculated_root = os.path.dirname(os.path.dirname(current_file))
        else:
            # Fallback
            calculated_root = os.path.dirname(current_file)
        
        status = "✅" if calculated_root == expected_root else "❌"
        print(f"{status} {current_file}")
        print(f"   Expected: {expected_root}")
        print(f"   Got:      {calculated_root}")
        
        if calculated_root != expected_root:
            return False
    
    return True

def main():
    """Run all verifications"""
    print("\n" + "🔍 PIPELINE SERVICE VERIFICATION" + "\n")
    
    results = []
    
    # Test 1: Column names
    try:
        results.append(("Column Names", verify_column_names()))
    except Exception as e:
        print(f"❌ Error verifying column names: {e}")
        results.append(("Column Names", False))
    
    # Test 2: Project root detection
    try:
        results.append(("Project Root Detection", verify_project_root()))
    except Exception as e:
        print(f"❌ Error verifying project root: {e}")
        results.append(("Project Root Detection", False))
    
    # Test 3: Path simulation
    try:
        results.append(("Path Simulation", simulate_path_detection()))
    except Exception as e:
        print(f"❌ Error simulating paths: {e}")
        results.append(("Path Simulation", False))
    
    # Summary
    print("\n" + "=" * 60)
    print("VERIFICATION SUMMARY")
    print("=" * 60)
    
    all_passed = True
    for name, passed in results:
        status = "✅ PASS" if passed else "❌ FAIL"
        print(f"{status}: {name}")
        if not passed:
            all_passed = False
    
    print("=" * 60)
    if all_passed:
        print("✅ ALL VERIFICATIONS PASSED")
        return 0
    else:
        print("❌ SOME VERIFICATIONS FAILED")
        return 1

if __name__ == "__main__":
    sys.exit(main())
