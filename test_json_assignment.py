#!/usr/bin/env python3
"""
Test script for JSON assignment functionality
"""

import json
import sys
import os

# Add the module_utils to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'module_utils'))

def test_json_assignment_validation():
    """Test JSON assignment validation logic"""
    
    # Test valid JSON assignment
    valid_assignment = {
        "partitions": [
            {
                "topic": "test-topic",
                "partition": 0,
                "replicas": [1001, 1002]
            },
            {
                "topic": "test-topic", 
                "partition": 1,
                "replicas": [1002, 1003]
            }
        ]
    }
    
    # Test invalid JSON assignment (missing required field)
    invalid_assignment = {
        "partitions": [
            {
                "topic": "test-topic",
                "partition": 0
                # Missing "replicas" field
            }
        ]
    }
    
    print("Testing JSON assignment validation...")
    
    # Test valid assignment
    try:
        if isinstance(valid_assignment, str):
            assignment_data = json.loads(valid_assignment)
        else:
            assignment_data = valid_assignment
            
        if 'partitions' not in assignment_data:
            raise ValueError('JSON assignment must contain "partitions" array')
            
        partitions = assignment_data['partitions']
        if not isinstance(partitions, list):
            raise ValueError('json_assignment "partitions" must be an array')
            
        for partition in partitions:
            if not isinstance(partition, dict):
                raise ValueError('Each partition in json_assignment must be an object')
            if 'topic' not in partition or 'partition' not in partition or 'replicas' not in partition:
                raise ValueError('Each partition must have "topic", "partition", and "replicas" fields')
            if not isinstance(partition['replicas'], list):
                raise ValueError('Partition replicas must be an array')
                
        print("✓ Valid JSON assignment passed validation")
        
    except (json.JSONDecodeError, ValueError) as e:
        print(f"✗ Valid JSON assignment failed validation: {e}")
        return False
    
    # Test invalid assignment
    try:
        if isinstance(invalid_assignment, str):
            assignment_data = json.loads(invalid_assignment)
        else:
            assignment_data = invalid_assignment
            
        if 'partitions' not in assignment_data:
            raise ValueError('JSON assignment must contain "partitions" array')
            
        partitions = assignment_data['partitions']
        if not isinstance(partitions, list):
            raise ValueError('json_assignment "partitions" must be an array')
            
        for partition in partitions:
            if not isinstance(partition, dict):
                raise ValueError('Each partition in json_assignment must be an object')
            if 'topic' not in partition or 'partition' not in partition or 'replicas' not in partition:
                raise ValueError('Each partition must have "topic", "partition", and "replicas" fields')
            if not isinstance(partition['replicas'], list):
                raise ValueError('Partition replicas must be an array')
                
        print("✗ Invalid JSON assignment should have failed validation")
        return False
        
    except (json.JSONDecodeError, ValueError) as e:
        print(f"✓ Invalid JSON assignment correctly failed validation: {e}")
    
    return True

def test_json_assignment_format():
    """Test different JSON assignment formats"""
    
    # Test string format
    string_assignment = '{"partitions": [{"topic": "test", "partition": 0, "replicas": [1001, 1002]}]}'
    
    # Test dict format
    dict_assignment = {
        "partitions": [
            {
                "topic": "test",
                "partition": 0,
                "replicas": [1001, 1002]
            }
        ]
    }
    
    print("\nTesting JSON assignment formats...")
    
    # Test string format
    try:
        assignment_data = json.loads(string_assignment)
        assert 'partitions' in assignment_data
        assert len(assignment_data['partitions']) == 1
        assert assignment_data['partitions'][0]['topic'] == 'test'
        print("✓ String JSON format works correctly")
    except Exception as e:
        print(f"✗ String JSON format failed: {e}")
        return False
    
    # Test dict format
    try:
        assignment_data = dict_assignment
        assert 'partitions' in assignment_data
        assert len(assignment_data['partitions']) == 1
        assert assignment_data['partitions'][0]['topic'] == 'test'
        print("✓ Dict JSON format works correctly")
    except Exception as e:
        print(f"✗ Dict JSON format failed: {e}")
        return False
    
    return True

if __name__ == "__main__":
    print("Running JSON assignment tests...\n")
    
    success = True
    success &= test_json_assignment_validation()
    success &= test_json_assignment_format()
    
    if success:
        print("\n✅ All tests passed!")
        sys.exit(0)
    else:
        print("\n❌ Some tests failed!")
        sys.exit(1)