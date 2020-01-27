import producer_server


def run_kafka_server():
	# Get the json file path
    input_file = "./police-department-calls-for-service.json"

    # Fill in blanks
    producer = producer_server.ProducerServer(
        input_file=input_file,
        topic="com.udacity.police.calls",
        bootstrap_servers=["localhost:9092"],
        client_id="kafka-python-producer"
    )

    return producer


def feed():
    producer = run_kafka_server()
    producer.generate_data()


if __name__ == "__main__":
    feed()
