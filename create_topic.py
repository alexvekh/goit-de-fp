from kafka.admin import KafkaAdminClient, NewTopic
import config

# Визначення нового топіку
topic_name = "vekh__aggregated_results"
num_partitions = 2
replication_factor = 1

admin_client = KafkaAdminClient(
    bootstrap_servers=config.kafka_url,
    security_protocol=config.security_protocol,
    sasl_mechanism=config.sasl_mechanism,
    sasl_plain_username=config.kafka_user,
    sasl_plain_password=config.kafka_password
)

new_topic = NewTopic(name=topic_name, num_partitions=num_partitions, replication_factor=replication_factor)

# Створення нового топіку
try:
    admin_client.create_topics(new_topics=[new_topic], validate_only=False)
    print(f"Topic '{topic_name}' created successfully.")
except Exception as e:
    print(f"An error occurred: {e}")

# Перевіряємо список існуючих топіків
print(admin_client.list_topics())

# Закриття зв'язку з клієнтом
admin_client.close()
