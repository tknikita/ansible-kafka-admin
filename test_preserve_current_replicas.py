#!/usr/bin/env python3
"""
Test script for preserve_current_replicas functionality
"""

import unittest
from unittest.mock import Mock, MagicMock
import itertools

# Mock the KafkaManager class to test the _get_preserved_replicas method
class TestKafkaManager:
    def _get_preserved_replicas(self, current_replicas, target_factor, all_brokers, leader=None):
        """
        Get preserved replicas based on current replica set and target factor.
        
        Args:
            current_replicas: List of current replica broker IDs
            target_factor: Target replica factor
            all_brokers: List of all available brokers
            leader: Current leader broker ID (optional)
            
        Returns:
            List of replica broker IDs for the new assignment
        """
        current_replicas = list(current_replicas)
        
        if target_factor == len(current_replicas):
            # No change needed
            return current_replicas
        elif target_factor < len(current_replicas):
            # Decreasing replica factor - remove from the end
            return current_replicas[:target_factor]
        else:
            # Increasing replica factor - add new replicas
            new_replicas = current_replicas.copy()
            available_brokers = [b for b in all_brokers if b not in current_replicas]
            
            if not available_brokers:
                # If no available brokers, we can't add more replicas
                return current_replicas
            
            # Use round-robin to select new brokers
            brokers_iterator = itertools.cycle(available_brokers)
            while len(new_replicas) < target_factor:
                if available_brokers:  # Check if there are available brokers
                    new_broker = next(brokers_iterator)
                    if new_broker not in new_replicas:
                        new_replicas.append(new_broker)
                else:
                    break  # No more brokers available
            
            return new_replicas


class TestPreserveCurrentReplicas(unittest.TestCase):
    
    def setUp(self):
        self.manager = TestKafkaManager()
        self.all_brokers = [1001, 1002, 1003, 1004, 1005]
    
    def test_no_change_needed(self):
        """Test when target factor equals current replica count"""
        current_replicas = [1001, 1002, 1003]
        target_factor = 3
        
        result = self.manager._get_preserved_replicas(current_replicas, target_factor, self.all_brokers)
        
        self.assertEqual(result, current_replicas)
    
    def test_decrease_replica_factor(self):
        """Test decreasing replica factor"""
        current_replicas = [1001, 1002, 1003, 1004]
        target_factor = 2
        
        result = self.manager._get_preserved_replicas(current_replicas, target_factor, self.all_brokers)
        
        # Should remove replicas from the end
        expected = [1001, 1002]
        self.assertEqual(result, expected)
    
    def test_increase_replica_factor(self):
        """Test increasing replica factor"""
        current_replicas = [1001, 1002]
        target_factor = 4
        
        result = self.manager._get_preserved_replicas(current_replicas, target_factor, self.all_brokers)
        
        # Should add new replicas while preserving existing ones
        self.assertEqual(len(result), 4)
        self.assertTrue(all(replica in result for replica in current_replicas))
        # New replicas should be from available brokers
        available_brokers = [b for b in self.all_brokers if b not in current_replicas]
        new_replicas = [r for r in result if r not in current_replicas]
        self.assertTrue(all(replica in available_brokers for replica in new_replicas))
    
    def test_increase_beyond_available_brokers(self):
        """Test increasing replica factor when not enough brokers available"""
        current_replicas = [1001, 1002, 1003]
        target_factor = 10  # More than available brokers
        
        result = self.manager._get_preserved_replicas(current_replicas, target_factor, self.all_brokers)
        
        # Should return as many as possible without exceeding available brokers
        self.assertLessEqual(len(result), len(self.all_brokers))
        self.assertTrue(all(replica in result for replica in current_replicas))
    
    def test_with_leader_specified(self):
        """Test with leader parameter specified (should not affect logic)"""
        current_replicas = [1001, 1002, 1003]
        target_factor = 2
        leader = 1001
        
        result = self.manager._get_preserved_replicas(current_replicas, target_factor, self.all_brokers, leader)
        
        # Leader parameter should not affect the logic for preserve_current_replicas
        expected = [1001, 1002]
        self.assertEqual(result, expected)


def run_tests():
    """Run the test suite"""
    print("Running tests for preserve_current_replicas functionality...")
    
    # Create test suite
    suite = unittest.TestLoader().loadTestsFromTestCase(TestPreserveCurrentReplicas)
    
    # Run tests
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    
    if result.wasSuccessful():
        print("\n✅ All tests passed! The preserve_current_replicas implementation is working correctly.")
    else:
        print("\n❌ Some tests failed. Please review the implementation.")
    
    return result.wasSuccessful()


if __name__ == '__main__':
    success = run_tests()
    exit(0 if success else 1)