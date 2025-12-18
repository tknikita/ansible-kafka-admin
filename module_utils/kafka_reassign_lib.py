# -*- coding: utf-8 -*-
"""
Kafka JSON Assignment Library
Contains all logic for JSON-based partition reassignment
"""
import json
from ansible.module_utils.kafka_lib_errors import KafkaManagerError


class JsonAssignmentValidator:
    """Validates JSON assignment parameters and format"""
    
    @staticmethod
    def validate_assignment_format(json_assignment, module=None):
        """
        Validate JSON assignment format and structure
        
        Args:
            json_assignment: JSON assignment data (dict or string)
            module: Ansible module for error reporting (optional)
            
        Returns:
            dict: Parsed assignment data
            
        Raises:
            KafkaManagerError: If validation fails
        """
        try:
            if isinstance(json_assignment, str):
                assignment_data = json.loads(json_assignment)
            else:
                assignment_data = json_assignment
        except (json.JSONDecodeError, ValueError) as e:
            error_msg = 'Invalid JSON assignment format: %s' % str(e)
            if module:
                module.fail_json(msg=error_msg)
            raise KafkaManagerError(error_msg)
        
        if not isinstance(assignment_data, dict):
            error_msg = 'json_assignment must be a JSON object'
            if module:
                module.fail_json(msg=error_msg)
            raise KafkaManagerError(error_msg)
        
        if 'partitions' not in assignment_data:
            error_msg = 'json_assignment must contain "partitions" array'
            if module:
                module.fail_json(msg=error_msg)
            raise KafkaManagerError(error_msg)
        
        partitions = assignment_data['partitions']
        if not isinstance(partitions, list):
            error_msg = 'json_assignment "partitions" must be an array'
            if module:
                module.fail_json(msg=error_msg)
            raise KafkaManagerError(error_msg)
        
        # Allow empty partitions array for status check
        # The actual status check logic is in the main function
        
        for i, partition in enumerate(partitions):
            if not isinstance(partition, dict):
                error_msg = 'Partition %d in json_assignment must be an object' % i
                if module:
                    module.fail_json(msg=error_msg)
                raise KafkaManagerError(error_msg)
            
            required_fields = ['topic', 'partition', 'replicas']
            for field in required_fields:
                if field not in partition:
                    error_msg = 'Partition %d must have "%s" field' % (i, field)
                    if module:
                        module.fail_json(msg=error_msg)
                    raise KafkaManagerError(error_msg)
            
            if not isinstance(partition['replicas'], list):
                error_msg = 'Partition %d replicas must be an array' % i
                if module:
                    module.fail_json(msg=error_msg)
                raise KafkaManagerError(error_msg)
            
            if len(partition['replicas']) == 0:
                error_msg = 'Partition %d replicas array cannot be empty' % i
                if module:
                    module.fail_json(msg=error_msg)
                raise KafkaManagerError(error_msg)
        
        return assignment_data
    
    @staticmethod
    def validate_broker_availability(partitions, manager):
        """
        Validate that all brokers in assignments are available
        
        Args:
            partitions: List of partition assignments
            manager: KafkaManager instance
            
        Raises:
            KafkaManagerError: If any broker is unavailable
        """
        available_brokers = set()
        for broker in manager.get_brokers():
            available_brokers.add(broker.nodeId)
        
        for partition in partitions:
            replicas = partition['replicas']
            unavailable_brokers = []
            
            for broker_id in replicas:
                if broker_id not in available_brokers:
                    unavailable_brokers.append(broker_id)
            
            if unavailable_brokers:
                raise KafkaManagerError(
                    'Unable to proceed with partition reassignment: '
                    'broker(s) %s in partition %d assignment for topic "%s" are not available. '
                    'Available brokers: %s' % (
                        unavailable_brokers,
                        partition['partition'],
                        partition['topic'],
                        sorted(available_brokers)
                    )
                )


class JsonAssignmentProcessor:
    """Processes and applies JSON assignments"""
    
    @staticmethod
    def parse_assignment(json_assignment):
        """
        Parse string/dict to consistent format
        
        Args:
            json_assignment: JSON assignment (string or dict)
            
        Returns:
            dict: Parsed assignment data
        """
        if isinstance(json_assignment, str):
            return json.loads(json_assignment)
        else:
            return json_assignment
    
    @staticmethod
    def apply_assignment(manager, json_assignment, wait_for_completion=True):
        """
        Apply JSON assignment to Kafka cluster
        
        Args:
            manager: KafkaManager instance
            json_assignment: JSON assignment data
            wait_for_completion: Whether to wait for reassignment completion
            
        Raises:
            KafkaManagerError: If application fails
        """
        from pkg_resources import parse_version
        from ansible.module_utils.kafka_protocol import AlterPartitionReassignmentsRequest_v0
        
        assignment_data = JsonAssignmentProcessor.parse_assignment(json_assignment)
        partitions = assignment_data['partitions']
        
        # Validate broker availability
        JsonAssignmentValidator.validate_broker_availability(partitions, manager)
        
        # Group partitions by topic for efficient processing
        topics_to_partitions = {}
        for partition in partitions:
            topic_name = partition['topic']
            if topic_name not in topics_to_partitions:
                topics_to_partitions[topic_name] = []
            topics_to_partitions[topic_name].append(partition)
        
        # Apply assignment based on Kafka version
        if parse_version(manager.get_api_version()) >= parse_version('2.4.0'):
            JsonAssignmentProcessor._apply_assignment_new_api(manager, topics_to_partitions, wait_for_completion)
        elif manager.zk_configuration is not None:
            JsonAssignmentProcessor._apply_assignment_zookeeper(manager, assignment_data, wait_for_completion)
        else:
            raise KafkaManagerError('Zookeeper is mandatory for partition assignment when using Kafka <= 2.4.0.')
        
        manager.refresh()
    
    @staticmethod
    def _apply_assignment_new_api(manager, topics_to_partitions, wait_for_completion):
        """Apply assignment using Kafka >= 2.4.0 API"""
        from pkg_resources import parse_version
        from ansible.module_utils.kafka_protocol import AlterPartitionReassignmentsRequest_v0
        
        # Build assignment request
        assign = []
        for topic_name, partitions in topics_to_partitions.items():
            partition_assignments = []
            for partition in partitions:
                partition_assignments.append((
                    partition['partition'],
                    partition['replicas'],
                    {}
                ))
            assign.append((topic_name, partition_assignments, {}))
        
        if assign:
            request = AlterPartitionReassignmentsRequest_v0(
                timeout_ms=manager.request_timeout_ms,
                topics=assign,
                tags={}
            )
            
            if wait_for_completion:
                manager.wait_for_partition_assignement()
            
            manager.send_request_and_get_response(request)
            
            if wait_for_completion:
                manager.wait_for_partition_assignement()
    
    @staticmethod
    def _apply_assignment_zookeeper(manager, assignment_data, wait_for_completion):
        """Apply assignment using ZooKeeper (older Kafka versions)"""
        try:
            manager.init_zk_client()
            
            if wait_for_completion:
                manager.wait_for_znode_assignment()
            
            # Create ZooKeeper format assignment
            zk_assignment = {
                'version': 1,
                'partitions': assignment_data['partitions']
            }
            
            manager.zk_client.create(
                manager.ZK_REASSIGN_NODE,
                json.dumps(zk_assignment, ensure_ascii=False).encode('utf-8')
            )
            
            if wait_for_completion:
                manager.wait_for_znode_assignment()
        finally:
            manager.close_zk_client()


class ReassignmentManager:
    """High-level interface for partition reassignment operations"""
    
    def __init__(self, manager):
        self.manager = manager
        self.validator = JsonAssignmentValidator()
        self.processor = JsonAssignmentProcessor()
    
    def validate_assignment(self, json_assignment, module=None):
        """Validate a JSON assignment"""
        return self.validator.validate_assignment_format(json_assignment, module)
    
    def apply_assignment(self, json_assignment, wait_for_completion=True):
        """Apply a JSON assignment to the cluster"""
        self.processor.apply_assignment(self.manager, json_assignment, wait_for_completion)
    
    def get_assignment_status(self):
        """Get current reassignment status"""
        from pkg_resources import parse_version
        from ansible.module_utils.kafka_protocol import ListPartitionReassignmentsRequest_v0
        
        if parse_version(self.manager.get_api_version()) >= parse_version('2.4.0'):
            request = ListPartitionReassignmentsRequest_v0(
                timeout_ms=self.manager.request_timeout_ms,
                topics=None,
                tags={}
            )
            response = self.manager.send_request_and_get_response(request)
            return response.to_object()
        else:
            # For older versions, check if znode exists
            if self.manager.zk_configuration is not None:
                try:
                    self.manager.init_zk_client()
                    exists = self.manager.zk_client.exists(self.manager.ZK_REASSIGN_NODE)
                    return {'reassignment_in_progress': exists}
                finally:
                    self.manager.close_zk_client()
            else:
                return {'reassignment_in_progress': False}