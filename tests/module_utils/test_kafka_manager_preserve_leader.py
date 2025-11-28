"""
Tests for preserve_leader functionality in kafka_manager
"""

import unittest
from unittest.mock import Mock, MagicMock, patch
import module_utils.kafka_manager as kafka_manager


class TestPreserveLeaderFunctionality(unittest.TestCase):
    """
    Test cases for the improved preserve_leader functionality
    """

    def setUp(self):
        """Set up test fixtures"""
        self.mock_client = Mock()
        self.mock_client.cluster.brokers.return_value = [
            Mock(nodeId=1001), Mock(nodeId=1002), Mock(nodeId=1003)
        ]
        self.mock_client.cluster.controller = (1001, 'host1', 9092, None)
        
        # Create KafkaManager instance
        with patch('kafka.client_async.KafkaClient', return_value=self.mock_client):
            self.manager = kafka_manager.KafkaManager(
                bootstrap_servers='localhost:9092',
                request_timeout_ms=15000
            )

    def test_preserve_leader_decreasing_replica_factor(self):
        """
        Test that when decreasing replica factor with preserve_leader=True,
        the function preserves the leader and existing replicas, trimming from the end.
        """
        # Mock partition metadata
        mock_partitions = {
            0: ('test_topic', 0, 1001, [1001, 1002, 1003], [1001, 1002, 1003], None)
        }
        
        with patch.object(self.manager, 'get_partitions_for_topic', return_value=mock_partitions):
            topics = {
                'test_topic': {
                    'replica_factor': 2,
                    'preserve_leader': True
                }
            }
            topics_configuration = {
                ('test_topic', 0): [1001, 1002, 1003]  # Current assignment
            }
            
            result = self.manager.get_assignment_for_replica_factor_update(
                topics, topics_configuration
            )
            
            # Should preserve leader (1001) and first existing replica (1002)
            self.assertIsNotNone(result)
            self.assertEqual(len(result), 1)
            topic_name, partitions, _ = result[0]
            self.assertEqual(topic_name, 'test_topic')
            self.assertEqual(len(partitions), 1)
            
            partition_id, replicas, _ = partitions[0]
            self.assertEqual(partition_id, 0)
            self.assertEqual(replicas, [1001, 1002])  # Leader + existing replica

    def test_preserve_leader_increasing_replica_factor(self):
        """
        Test that when increasing replica factor with preserve_leader=True,
        the function preserves existing replicas and adds new ones.
        """
        # Mock partition metadata
        mock_partitions = {
            0: ('test_topic', 0, 1001, [1001, 1002], [1001, 1002], None)
        }
        
        with patch.object(self.manager, 'get_partitions_for_topic', return_value=mock_partitions):
            topics = {
                'test_topic': {
                    'replica_factor': 3,
                    'preserve_leader': True
                }
            }
            topics_configuration = {
                ('test_topic', 0): [1001, 1002]  # Current assignment
            }
            
            result = self.manager.get_assignment_for_replica_factor_update(
                topics, topics_configuration
            )
            
            # Should preserve existing replicas and add new one
            self.assertIsNotNone(result)
            self.assertEqual(len(result), 1)
            topic_name, partitions, _ = result[0]
            self.assertEqual(topic_name, 'test_topic')
            self.assertEqual(len(partitions), 1)
            
            partition_id, replicas, _ = partitions[0]
            self.assertEqual(partition_id, 0)
            self.assertEqual(len(replicas), 3)
            self.assertIn(1001, replicas)  # Leader preserved
            self.assertIn(1002, replicas)  # Existing replica preserved

    def test_preserve_leader_same_replica_factor(self):
        """
        Test that when replica factor is the same with preserve_leader=True,
        no changes are made.
        """
        # Mock partition metadata
        mock_partitions = {
            0: ('test_topic', 0, 1001, [1001, 1002], [1001, 1002], None)
        }
        
        with patch.object(self.manager, 'get_partitions_for_topic', return_value=mock_partitions):
            topics = {
                'test_topic': {
                    'replica_factor': 2,
                    'preserve_leader': True
                }
            }
            topics_configuration = {
                ('test_topic', 0): [1001, 1002]  # Current assignment
            }
            
            result = self.manager.get_assignment_for_replica_factor_update(
                topics, topics_configuration
            )
            
            # Should return None since no changes are needed
            self.assertIsNone(result)

    def test_preserve_leader_false_behavior_unchanged(self):
        """
        Test that when preserve_leader=False, the original behavior is preserved.
        """
        # Mock partition metadata
        mock_partitions = {
            0: ('test_topic', 0, 1001, [1001, 1002], [1001, 1002], None)
        }
        
        with patch.object(self.manager, 'get_partitions_for_topic', return_value=mock_partitions):
            topics = {
                'test_topic': {
                    'replica_factor': 3,
                    'preserve_leader': False
                }
            }
            topics_configuration = {
                ('test_topic', 0): [1001, 1002]  # Current assignment
            }
            
            result = self.manager.get_assignment_for_replica_factor_update(
                topics, topics_configuration
            )
            
            # Should generate new assignment using round-robin
            self.assertIsNotNone(result)
            self.assertEqual(len(result), 1)
            topic_name, partitions, _ = result[0]
            self.assertEqual(topic_name, 'test_topic')
            self.assertEqual(len(partitions), 1)
            
            partition_id, replicas, _ = partitions[0]
            self.assertEqual(partition_id, 0)
            self.assertEqual(len(replicas), 3)

    def test_preserve_leader_with_zk_decreasing_replica_factor(self):
        """
        Test the ZooKeeper version with decreasing replica factor.
        """
        # Mock partition metadata
        mock_partitions = {
            0: ('test_topic', 0, 1001, [1001, 1002, 1003], [1001, 1002, 1003], None)
        }
        
        with patch.object(self.manager, 'get_partitions_for_topic', return_value=mock_partitions):
            topics = {
                'test_topic': {
                    'replica_factor': 2,
                    'preserve_leader': True
                }
            }
            topics_configuration = {
                ('test_topic', 0): [1001, 1002, 1003]  # Current assignment
            }
            
            result = self.manager.get_assignment_for_replica_factor_update_with_zk(
                topics, topics_configuration
            )
            
            # Should preserve leader (1001) and first existing replica (1002)
            self.assertIsNotNone(result)
            import json
            assignment = json.loads(result.decode('utf-8'))
            self.assertEqual(assignment['version'], 1)
            self.assertEqual(len(assignment['partitions']), 1)
            
            partition_assignment = assignment['partitions'][0]
            self.assertEqual(partition_assignment['topic'], 'test_topic')
            self.assertEqual(partition_assignment['partition'], 0)
            self.assertEqual(partition_assignment['replicas'], [1001, 1002])

    def test_preserve_leader_no_current_assignment(self):
        """
        Test behavior when no current assignment information is available.
        """
        # Mock partition metadata
        mock_partitions = {
            0: ('test_topic', 0, 1001, [1001, 1002], [1001, 1002], None)
        }
        
        with patch.object(self.manager, 'get_partitions_for_topic', return_value=mock_partitions):
            topics = {
                'test_topic': {
                    'replica_factor': 3,
                    'preserve_leader': True
                }
            }
            topics_configuration = {}  # No current assignment info
            
            result = self.manager.get_assignment_for_replica_factor_update(
                topics, topics_configuration
            )
            
            # Should fall back to original logic
            self.assertIsNotNone(result)
            self.assertEqual(len(result), 1)
            topic_name, partitions, _ = result[0]
            self.assertEqual(topic_name, 'test_topic')
            self.assertEqual(len(partitions), 1)
            
            partition_id, replicas, _ = partitions[0]
            self.assertEqual(partition_id, 0)
            self.assertEqual(len(replicas), 3)
            self.assertEqual(replicas[0], 1001)  # Leader should be first


if __name__ == '__main__':
    unittest.main()