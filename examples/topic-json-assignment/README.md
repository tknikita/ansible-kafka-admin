# Topic JSON Assignment Example

This example demonstrates how to use the `json_assignment` parameter to manually specify partition-to-broker assignments for Kafka topics. This is particularly useful for draining brokers from topics or performing custom rebalancing operations.

## Use Cases

- **Draining brokers**: Remove a broker from all topic partitions before maintenance
- **Custom rebalancing**: Manually control partition distribution across brokers
- **Broker decommissioning**: Move all partitions away from a broker being retired
- **Selective reassignment**: Only reassign specific partitions while leaving others unchanged

## JSON Assignment Format

The `json_assignment` parameter accepts a JSON object with the following structure:

```json
{
  "partitions": [
    {
      "topic": "topic_name",
      "partition": 0,
      "replicas": [1001, 1002]
    },
    {
      "topic": "topic_name", 
      "partition": 1,
      "replicas": [1002, 1003]
    }
  ]
}
```

### Field Descriptions

- **topic**: The name of the topic (must match the topic being configured)
- **partition**: The partition ID (integer)
- **replicas**: Array of broker IDs that should host replicas for this partition

## Important Constraints

1. **Cannot be used with**: `partitions`, `replica_factor`, or `force_reassign` parameters
2. **Topic consistency**: All partitions in the assignment must belong to the same topic
3. **Broker validation**: Broker IDs must exist in the cluster
4. **Replica count**: The number of replicas should match your replication factor requirements

## Example: Draining a Broker

This example shows how to drain broker 1003 from all partitions of a topic:

```yaml
- name: "Drain broker 1003 from topic"
  kafka_topic:
    api_version: "2.6.0"
    name: "my-topic"
    bootstrap_servers: "localhost:9092,localhost:9093,localhost:9094"
    state: "present"
    json_assignment: 
      partitions:
        - topic: "my-topic"
          partition: 0
          replicas: [1001, 1002]  # Remove 1003
        - topic: "my-topic"
          partition: 1
          replicas: [1001, 1002]  # Remove 1003
        - topic: "my-topic"
          partition: 2
          replicas: [1001, 1002]  # Remove 1003
```

## Example: Selective Partition Reassignment

Only reassign specific partitions while leaving others unchanged:

```yaml
- name: "Reassign only partition 1 to brokers 1002, 1003"
  kafka_topic:
    api_version: "2.6.0"
    name: "my-topic"
    bootstrap_servers: "localhost:9092,localhost:9093,localhost:9094"
    state: "present"
    json_assignment:
      partitions:
        - topic: "my-topic"
          partition: 1
          replicas: [1002, 1003]
```

## Compatibility

- **Kafka >= 2.4.0**: Uses the native AlterPartitionReassignments API
- **Kafka < 2.4.0**: Requires ZooKeeper connection and uses the reassignment znode
- **ZooKeeper**: Required for Kafka versions below 2.4.0

## Error Handling

The module validates the JSON assignment format and will fail with clear error messages for:
- Invalid JSON syntax
- Missing required fields (topic, partition, replicas)
- Mismatched topic names
- Invalid data types
- Conflicting parameters

## Performance Considerations

- JSON assignments are applied atomically per topic
- Large assignments may take time to complete
- The module waits for reassignment completion before proceeding
- Consider using `kafka_sleep_time` and `kafka_max_retries` parameters for large clusters

## See Also

- [Kafka Partition Reassignment Documentation](https://kafka.apache.org/documentation/#basic_ops_cluster_expansion)
- [Broker Decommissioning Best Practices](https://kafka.apache.org/documentation/#operations)