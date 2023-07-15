import csv
from confluent_kafka import Consumer

# Kafka consumer configuration
consumer_config = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'my-consumer-group',
    'auto.offset.reset': 'earliest'
}

def consume_and_save_to_csv(topic, output_file):
    consumer = Consumer(consumer_config)
    consumer.subscribe([topic])

    try:
        with open(output_file, 'w', newline='') as file:
            writer = csv.writer(file)

            while True:
                msg = consumer.poll(1.0)  # Poll for new messages

                if msg is None:
                    continue

                if msg.error():
                    print(f"Consumer error: {msg.error()}")
                    continue

                value = msg.value().decode('utf-8')  # Extract the value of the message

                # Split the message by comma and save it as a row in the CSV file
                row = value.split(',')
                writer.writerow(row)

                print(value)  # Optional: Print the message

    except KeyboardInterrupt:
        consumer.close()

# Usage example
topic = 'mytopic'
output_file = 'Data/Ingested/train_kafka.csv'
consume_and_save_to_csv(topic, output_file)
