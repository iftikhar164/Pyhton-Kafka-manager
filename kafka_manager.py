import json
import logging
from kafka import KafkaConsumer, KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic, NewPartitions

from kafka.errors import TopicAlreadyExistsError, InvalidPartitionsError


class KafkaManager:
    def __init__(self, host="127.0.0.1", port=9092, logger=None, **kwargs):
        self.logger = logger
        if not self.logger:
            self.logger = logging.getLogger("kafka_manager")
        self.host = host
        self.port = port
        self.group_id = kwargs.get("group_id", 1)
        self.kafka_producer = self._kafka_producer()
        self.kafka_consumer = self._kafka_consumer()
        self.kafka_admin = self._kafka_admin()

    def delete_topics(self, topic_name_list, **kwargs):
        try:
            response = self.kafka_admin.delete_topics(topic_name_list)
            return True, response
        except Exception as ex:
            self.logger.error(f"Error in delete_topics topics: {topic_name_list}, error: {ex}")
            return False, ex

    def describe_topics(self, topic_name_list, **kwargs):
        try:
            response = self.kafka_admin.describe_topics(topic_name_list)
            return True, response
        except Exception as ex:
            self.logger.error(f"Error in describe_topics topics: {topic_name_list}, error: {ex}")
            return False, ex

    def create_partitions(self, topic_name, partitions_count=1, **kwargs):
        try:
            status, res = self.describe_topics([topic_name])
            if not status:
                return False, res

            topic_partitions = len(res[0]["partitions"])
            if partitions_count <= topic_partitions:
                return True, None
            response = self.kafka_admin.create_partitions({topic_name: NewPartitions(partitions_count)})
            return True, response
        except Exception as ex:
            self.logger.error(f"Error in create_partitions topic: {topic_name}, error: {ex}")
            return False, ex

    def create_topics(self, topic_name_ist, num_partitions=1, **kwargs):
        try:
            topics_list = list()
            for name in topic_name_ist:
                topics_list.append(
                    NewTopic(name=name, num_partitions=num_partitions, replication_factor=1))
            response = self.kafka_admin.create_topics(topics_list)
            return True, response.topic_errors

        except TopicAlreadyExistsError as ex:
            self.logger.warning(f"Topic in create_topics topics: {topic_name_ist}, msg: {ex}")
            return True, None

        except Exception as ex:
            self.logger.error(f"Error in create_topics topics: {topic_name_ist}, error: {ex}")
            return False, ex

    def _kafka_admin(self):
        admin = KafkaAdminClient(bootstrap_servers=[f"{self.host}:{self.port}"])
        return admin

    def _kafka_consumer(self):
        consumer = KafkaConsumer(
            bootstrap_servers=[f"{self.host}:{self.port}"],
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id=self.group_id,
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )

        return consumer

    def _kafka_producer(self):
        producer = KafkaProducer(
            bootstrap_servers=[f"{self.host}:{self.port}"],
            value_serializer=lambda x: json.dumps(x).encode('utf-8')
        )
        return producer

    def subscribe_topic(self, topic):

        if not self.kafka_consumer.subscription():
            self.kafka_consumer.subscribe([topic])

    def get_message(self, topic, timeout_ms=30000, max_records=1):

        kafka_message = None

        self.subscribe_topic(topic)

        for key, message in self.kafka_consumer.poll(timeout_ms=timeout_ms, max_records=max_records, update_offsets=True).items():
            message = message[0]
            kafka_message = message.value
            #self.kafka_consumer.commit_async()
           #self.kafka_consumer.commit(asynchronous=False)

        return kafka_message

    def publish_message(self, topic, message):
        self.kafka_producer.send(topic=topic, value=message)

# if __name__ == '__main__':
#     pass
