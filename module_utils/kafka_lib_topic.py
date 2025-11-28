# -*- coding: utf-8 -*-
import collections
import json
import traceback

from kafka.errors import KafkaError

from ansible.module_utils.kafka_lib_commons import (
    get_manager_from_params,
    maybe_clean_kafka_ssl_files,
    maybe_clean_zk_ssl_files
)
from ansible.module_utils.kafka_lib_errors import get_exception


def process_module_topics(module, params=None):
    params = params if params is not None else module.params

    topics = params['topics']
    mark_others_as_absent = params.get('mark_others_as_absent', False)

    # Check for duplicated topics
    duplicated_topics = [topic for topic, count in collections.Counter(
        [topic['name'] for topic in topics]
    ).items() if count > 1]

    if len(duplicated_topics) > 0:
        module.fail_json(
            msg='Got duplicated topics in \'topics\': %s' % duplicated_topics
        )
        return

    # Validate parameter conflicts for all topics
    for topic in topics:
        json_assignment = topic.get('json_assignment')
        
        # Validate preserve_leader and preserve_current_replicas conflicts (always)
        if topic.get('preserve_leader', False) and topic.get('preserve_current_replicas', False):
            module.fail_json(
                msg='Cannot use both preserve_leader and preserve_current_replicas for topic %s' % topic['name']
            )
            return
        
        # Validate json_assignment parameter conflicts
        if json_assignment is not None:
            # Check for conflicting parameters
            if topic.get('partitions', 0) > 0:
                module.fail_json(
                    msg='Cannot use json_assignment with partitions parameter for topic %s' % topic['name']
                )
                return
            if topic.get('replica_factor', 0) > 0:
                module.fail_json(
                    msg='Cannot use json_assignment with replica_factor parameter for topic %s' % topic['name']
                )
                return
            if topic.get('force_reassign', False):
                module.fail_json(
                    msg='Cannot use json_assignment with force_reassign parameter for topic %s' % topic['name']
                )
                return
            
            # json_assignment cannot be used with preserve_leader or preserve_current_replicas
            if topic.get('preserve_leader', False):
                module.fail_json(
                    msg='Cannot use json_assignment with preserve_leader parameter for topic %s' % topic['name']
                )
                return
            if topic.get('preserve_current_replicas', False):
                module.fail_json(
                    msg='Cannot use json_assignment with preserve_current_replicas parameter for topic %s' % topic['name']
                )
                return
            
            # Validate JSON assignment format
            try:
                if isinstance(json_assignment, str):
                    assignment_data = json.loads(json_assignment)
                else:
                    assignment_data = json_assignment
                
                if not isinstance(assignment_data, dict):
                    module.fail_json(
                        msg='json_assignment must be a JSON object for topic %s' % topic['name']
                    )
                    return
                
                if 'partitions' not in assignment_data:
                    module.fail_json(
                        msg='json_assignment must contain "partitions" array for topic %s' % topic['name']
                    )
                    return
                
                partitions = assignment_data['partitions']
                if not isinstance(partitions, list):
                    module.fail_json(
                        msg='json_assignment "partitions" must be an array for topic %s' % topic['name']
                    )
                    return
                
                for partition in partitions:
                    if not isinstance(partition, dict):
                        module.fail_json(
                            msg='Each partition in json_assignment must be an object for topic %s' % topic['name']
                        )
                        return
                    if 'topic' not in partition or 'partition' not in partition or 'replicas' not in partition:
                        module.fail_json(
                            msg='Each partition must have "topic", "partition", and "replicas" fields for topic %s' % topic['name']
                        )
                        return
                    if not isinstance(partition['replicas'], list):
                        module.fail_json(
                            msg='Partition replicas must be an array for topic %s' % topic['name']
                        )
                        return
                        
            except (json.JSONDecodeError, ValueError) as e:
                module.fail_json(
                    msg='Invalid JSON assignment format for topic %s: %s' % (topic['name'], str(e))
                )
                return

    changed = False
    msg = ''
    warn = None
    changes = {}
    manager = None

    try:
        manager = get_manager_from_params(params)
        current_topics = manager.get_topics()

        topics_to_create = [
            topic for topic in topics
            if (topic['state'] == 'present' and
                topic['name'] not in current_topics)
        ]
        if len(topics_to_create) > 0:
            if not module.check_mode:
                manager.create_topics(topics_to_create)
            changed = True
            msg += ''.join(['topic %s successfully created. ' %
                            topic['name'] for topic in topics_to_create])
            changes.update({
                'topic_created': topics_to_create
            })

        # Handle JSON assignments first
        topics_json_assignment = [
            topic for topic in topics
            if (topic['state'] == 'present' and
                topic['name'] in current_topics and
                topic.get('json_assignment') is not None)
        ]
        
        if len(topics_json_assignment) > 0:
            if not module.check_mode:
                for topic in topics_json_assignment:
                    manager.apply_json_assignment(
                        topic['name'],
                        topic['json_assignment']
                    )
            changed = True
            msg += ''.join(['topic %s successfully updated with JSON assignment. ' %
                           topic['name'] for topic in topics_json_assignment])
            changes.update({
                'topic_json_assignment_updated': [topic['name'] for topic in topics_json_assignment]
            })

        # Handle regular topic updates (excluding those with JSON assignments)
        topics_to_maybe_update = [
            topic for topic in topics
            if (topic['state'] == 'present' and
                topic['name'] in current_topics and
                topic.get('json_assignment') is None)
        ]
        if len(topics_to_maybe_update) > 0:
            if module.check_mode:
                # just discover topics that will change
                topics_changed, warn = manager.get_topics_to_update(
                    topics_to_maybe_update
                )
            else:
                # perform the changes
                topics_changed, warn = manager.ensure_topics(
                    topics_to_maybe_update
                )
            changed = changed or len(topics_changed) > 0
            if len(topics_changed) > 0:
                msg += ''.join(['topic %s successfully updated. ' %
                                topic for topic in topics_changed])
                changes.update({
                    'topic_updated': topics_changed
                })

        topics_to_delete = [
            topic for topic in topics
            if (topic['state'] == 'absent' and
                topic['name'] in current_topics)
        ]
        # Cleanup existing if necessary
        if mark_others_as_absent:
            defined_topics = [topic['name'] for topic in topics]
            for existing_topic in set(current_topics) - set(defined_topics):
                topics_to_delete.append({
                    'name': existing_topic,
                    'state': 'absent'
                })
        if len(topics_to_delete) > 0:
            if not module.check_mode:
                manager.delete_topics(topics_to_delete)
            changed = True
            msg += ''.join(['topic %s successfully deleted. ' %
                           topic['name'] for topic in topics_to_delete])
            changes.update({
                'topic_deleted': topics_to_delete
            })
    except KafkaError:
        e = get_exception()
        module.fail_json(
            msg='Unable to initialize Kafka manager: %s' % e
        )
    except Exception:
        e = get_exception()
        module.fail_json(
            msg='Something went wrong: (%s) %s' % (e, traceback.format_exc(e)),
            changes=changes
        )
    finally:
        if manager:
            manager.close()
        # Use cached SSL files from manager to avoid recreating them
        maybe_clean_kafka_ssl_files(params, getattr(manager, 'kafka_ssl_files', None))
        maybe_clean_zk_ssl_files(params, getattr(manager, 'zookeeper_ssl_files', None))

    if not changed:
        msg += 'nothing to do.'

    if warn is not None and len(warn) > 0:
        module.warn(warn)

    module.exit_json(changed=changed, msg=msg, changes=changes)


def process_module_topic(module):
    params = module.params.copy()
    params['topics'] = [{
        'name': params['name'],
        'partitions': params['partitions'],
        'replica_factor': params['replica_factor'],
        'force_reassign': params['force_reassign'],
        'preserve_leader': params['preserve_leader'],
        'preserve_current_replicas': params['preserve_current_replicas'],
        'json_assignment': params['json_assignment'],
        'state': params['state'],
        'options': params['options']
    }]

    process_module_topics(module, params)
