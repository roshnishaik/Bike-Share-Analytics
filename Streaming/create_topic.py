from kafka.admin import KafkaAdminClient, KafkaConsumer, NewTopic

admin_client = KafkaAdminClient(bootstrap_servers=['199.60.17.210', '199.60.17.193'], client_id='test')

topic = "citi1"
topic_list = []
topic_list.append(NewTopic(name=topic, num_partitions=1, replication_factor=1))
admin_client.create_topics(new_topics=topic_list, validate_only=False)
