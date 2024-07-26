from kazoo.client import KazooClient
import json


main_conf_stagging = {
    "KafkaManager": {
      "host": "localhost",
      "port": 9092
      }
}

main_conf_path = "/KafkaManager/config"


def write_config(zk_object, path="/", value=b''):
    try:
        if isinstance(value, dict):
            value = json.dumps(value).encode("utf-8")
        elif isinstance(value, str):
            value = value.encode("utf-8")
        elif isinstance(value, int):
            value = value.to_bytes(8, "big")
        node_stats = zk_object.set(path, value)
        return True, node_stats
    except Exception as ex:
        print(f"Error while updating node: {path} value, error: {ex}.")
        return False, str(ex)


def read_config(zk_object, path="/"):
    try:
        data, stats = zk_object.get(path)
        return True, data, stats
    except Exception as ex:
        print(f"Error while getting node: {path} value, error: {ex}.")
        return False, None, str(ex)


def ensure_path(zk_object, path):
    try:
        path = zk_object.ensure_path(path)
        return True, path
    except Exception as ex:
        print(f"Error in ensure_path path: {path}, error: {ex}.")
        return False, str(ex)


if __name__ == '__main__':
    zk_manager = KazooClient(hosts="127.0.0.1:2181")
    zk_manager.start()
    ensure_path(zk_manager, main_conf_path)
    status, stats = write_config(zk_manager, main_conf_path, json.dumps(main_conf_stagging).encode("utf-8"))
    status, data, stats = read_config(zk_manager, main_conf_path)
    print(json.dumps(json.loads(data)))

