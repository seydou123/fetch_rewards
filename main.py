from confluent_kafka import Consumer, Producer

from user_login.consume import process_messages

# Define the Kafka Consumer
consumer = Consumer(
    {
        'bootstrap.servers': 'kafka:9092',
        'group.id': 'user-login-consumer',
        'auto.offset.reset': 'earliest',  # Start from the earliest message
    }
)

# Define the Kafka Producer for the processed data
producer = Producer({'bootstrap.servers': 'kafka:9092'})

if __name__ == "__main__":

    # Consumer subscribes to the user-login topic
    consumer.subscribe(['user-login'])

    # Process the data to another topic
    process_messages(consumer, producer)
