import json
from kafka import KafkaProducer
import argparse
import sys
import time

sample_data = {
    "data_key": "sample data for my producer"
}
if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    # Adding optional argument
    parser.add_argument("-n", "--run_count", help="Show Output")
    args = parser.parse_args()
    if not args.run_count:
        sys.exit()

    request_count = int(args.run_count)
    producer = KafkaProducer(bootstrap_servers='127.0.0.1:9092',
                             value_serializer=lambda x: json.dumps(x).encode('utf-8'))

    total_search_time = 0
    example_topic = 'test_run_1'
    print("going to produce sample data: {}".format(sample_data))
    for i in range(request_count):
        start_time = time.time()
        producer.send(example_topic, sample_data)
        total_search_time += (time.time() - start_time)
        # print(i)
    producer.close()

    print(f"total run time in sec: {total_search_time}")
    print(f"total number of requests: {request_count}")
    print(f"Avg request/sec: {total_search_time / request_count}")
