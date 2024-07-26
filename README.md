Pyhton Kafka Manager for common operations
The KafkaManager class is a Python-based utility for managing Kafka topics, partitions, and messages. It provides a high-level interface to Kafka functionalities such as topic creation, partition management, message production, and consumption. The class uses Kafka's producer, consumer, and admin clients to interact with a Kafka cluster. Below is a detailed description of the code:

Class Initialization
__init__
The __init__ method initializes the KafkaManager instance with the following parameters:

host: The Kafka broker host (default: "127.0.0.1").
port: The Kafka broker port (default: 9092).
logger: A logger instance for logging messages. If not provided, it defaults to a logger named "kafka_manager".
**kwargs: Additional keyword arguments, including group_id for the Kafka consumer group.
The method initializes the Kafka producer, consumer, and admin clients by calling the private methods _kafka_producer(), _kafka_consumer(), and _kafka_admin().

Private Methods
_kafka_admin
This method creates and returns a KafkaAdminClient instance, which is used for administrative operations on the Kafka cluster.

_kafka_consumer
This method creates and returns a KafkaConsumer instance with the following configurations:

bootstrap_servers: The Kafka broker address.
auto_offset_reset: Resets the offset to the earliest available message if no offset is present (default: 'earliest').
enable_auto_commit: Enables automatic offset commits (default: True).
group_id: The consumer group ID.
value_deserializer: A lambda function to deserialize JSON-encoded messages.
_kafka_producer
This method creates and returns a KafkaProducer instance with the following configurations:

bootstrap_servers: The Kafka broker address.
value_serializer: A lambda function to serialize messages to JSON format.
Public Methods
delete_topics
Deletes the specified topics from the Kafka cluster.

topic_name_list: A list of topic names to delete.
Returns a tuple (status, response) where status is a boolean indicating success, and response contains the result or error message.
describe_topics
Describes the specified topics.

topic_name_list: A list of topic names to describe.
Returns a tuple (status, response) where status is a boolean indicating success, and response contains the result or error message.
create_partitions
Creates partitions for a specified topic.

topic_name: The name of the topic.
partitions_count: The desired number of partitions (default: 1).
Returns a tuple (status, response) where status is a boolean indicating success, and response contains the result or error message.
create_topics
Creates the specified topics.

topic_name_list: A list of topic names to create.
num_partitions: The number of partitions for each topic (default: 1).
Returns a tuple (status, response) where status is a boolean indicating success, and response contains the result or error message.
subscribe_topic
Subscribes the Kafka consumer to a specified topic if not already subscribed.

topic: The name of the topic to subscribe to.
get_message
Fetches a message from the specified topic.

topic: The name of the topic to fetch the message from.
timeout_ms: The maximum time in milliseconds to block waiting for messages (default: 30000).
max_records: The maximum number of records to fetch (default: 1).
Returns the fetched message.
publish_message
Publishes a message to the specified topic.

topic: The name of the topic to publish the message to.
message: The message to publish.
Example Usage
python
Copy code
if __name__ == '__main__':
    kafka_manager = KafkaManager()

    # Create topics
    status, response = kafka_manager.create_topics(['test_topic'], num_partitions=3)
    print("Create Topics:", status, response)

    # Describe topics
    status, response = kafka_manager.describe_topics(['test_topic'])
    print("Describe Topics:", status, response)

    # Create partitions
    status, response = kafka_manager.create_partitions('test_topic', partitions_count=5)
    print("Create Partitions:", status, response)

    # Publish a message
    kafka_manager.publish_message('test_topic', {'key': 'value'})
    print("Message Published")

    # Get a message
    message = kafka_manager.get_message('test_topic')
    print("Message Received:", message)
This code provides a comprehensive interface for managing Kafka resources and interacting with Kafka topics programmatically. It encapsulates common Kafka operations, handling errors gracefully and logging messages for debugging purposes.
