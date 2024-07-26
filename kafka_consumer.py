from kafka import KafkaConsumer, OffsetAndMetadata, TopicPartition
import json
import time
import argparse
import sys

if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    # Adding optional argument
    parser.add_argument("-n", "--run_count", help="Show Output")
    args = parser.parse_args()
    if not args.run_count:
        sys.exit()

    request_count = int(args.run_count)
    consumer = KafkaConsumer(
        bootstrap_servers=['127.0.0.1:9092'],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id="2",
        auto_commit_interval_ms=5,
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    consumer.subscribe(["raw_data"])

    count = 0
    total_search_time = 0
    for i in range(request_count):
        start_time = time.time()
        kafka_message = None
        for key, message in consumer.poll(timeout_ms=1000, max_records=1,
                                          update_offsets=True).items():
            message = message[0]
            kafka_message = message.value
        total_search_time += (time.time() - start_time)
        print(kafka_message)
        if not kafka_message:
            print(f"Empty Topic")


    print(f"total run time in sec: {total_search_time}")
    print(f"total number of requests: {request_count}")
    print(f"Avg request/sec: {total_search_time / request_count}")
