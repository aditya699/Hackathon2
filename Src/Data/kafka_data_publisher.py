from kafka import KafkaProducer

# Kafka producer configuration
bootstrap_servers = 'localhost:9092'
topic = 'house_price'

# Create a Kafka producer instance
producer = KafkaProducer(bootstrap_servers=bootstrap_servers)

# Path to your CSV file
csv_file = 'Data/Processed/train_cleaned.csv'

# Read the CSV file and push each line as a message to the Kafka topic
with open(csv_file, 'r') as file:
    for line in file:
        # Encode the message as bytes before sending
        message = line.encode('utf-8')
        producer.send(topic, value=message)

# Close the Kafka producer
producer.close()
