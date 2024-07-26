from kafka.admin import KafkaAdminClient, NewTopic, NewPartitions
from kafka.errors import TopicAlreadyExistsError


def describe_topics(kafka_admin, topic_name_list):
    try:
        response = kafka_admin.describe_topics(topic_name_list)
        return True, response
    except Exception as ex:
        print(f"Error in describe_topics topics: {topic_name_list}, error: {ex}")
        return False, ex


def create_partitions(kafka_admin, topic_name, partitions_count=1):
    try:
        status, res = describe_topics(kafka_admin, [topic_name])
        if not status:
            return False, res

        topic_partitions = len(res[0]["partitions"])
        if partitions_count <= topic_partitions:
            return True, None
        response = kafka_admin.create_partitions({topic_name: NewPartitions(partitions_count)})
        return True, response
    except Exception as ex:
        print(f"Error in create_partitions topic: {topic_name}, error: {ex}")
        return False, ex


def create_topics(kafka_admin, topic_name_ist, num_partitions=1, replication_factor=1):
    try:
        topics_list = list()
        for name in topic_name_ist:
            topics_list.append(
                NewTopic(name=name, num_partitions=num_partitions, replication_factor=replication_factor))
        response = kafka_admin.create_topics(topics_list)
        return True, response.topic_errors
    except TopicAlreadyExistsError as ex:
        print(f"Topic in create_topics topics: {topic_name_ist}, msg: {ex}")
        return True, None

    except Exception as ex:
        print(f"Error in create_topics topics: {topic_name_ist}, error: {ex}")
        return False, ex


def delete_topics(kafka_admin, topic_name_list):
    try:
        response = kafka_admin.delete_topics(topic_name_list)
        return True, response
    except Exception as ex:
        print(f"Error in delete_topics topics: {topic_name_list}, error: {ex}")
        return False, ex


if __name__ == '__main__':
    admin = KafkaAdminClient(bootstrap_servers=["cluster.local:9092"])
    topic_data_list = [{"topic_name": "raw_data", "partitions": 20}, {"topic_name": "after_cim", "partitions": 10},
                       {"topic_name": "after_enrichment", "partitions": 10}]
    for topic_data in topic_data_list:
        topic_name = topic_data["topic_name"]
        partitions = topic_data["partitions"]
        # print(delete_topics(admin, [topic_name]))
        print(create_topics(admin, [topic_name], partitions))
        print(create_partitions(admin, topic_name, partitions))
        print(describe_topics(admin, [topic_name]))
