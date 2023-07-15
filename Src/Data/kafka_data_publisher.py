from confluent_kafka import Producer
import csv

# Kafka producer configuration
producer_config = {
    'bootstrap.servers': 'localhost:9092',
    'client.id': 'my-client-id'
}

def publish_csv_to_kafka(file_path, topic):
    producer = Producer(producer_config)

    with open(file_path, 'r') as file:
        csv_reader = csv.reader(file)
        header = next(csv_reader)  # Read the header line

        for row in csv_reader:
            # Convert the row to a string
            data = ','.join(row)
            
            # Publish the data to the Kafka topic
            producer.produce(topic, value=data)

    producer.flush()

# Usage example
file_path = "Data/Processed/train_cleaned.csv"
topic = "mytopic"
publish_csv_to_kafka(file_path, topic)
