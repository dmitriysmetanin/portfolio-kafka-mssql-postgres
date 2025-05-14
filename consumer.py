from confluent_kafka import Consumer

# Конфиг Kafka Consumer
conf = {
    'bootstrap.servers': 'kafka:9092',  # Адрес брокеров Kafka
    'group.id': 'consumer-group-1',     # Группа Consumer-ов
    'auto.offset.reset': 'earliest'     # Старт чтения с начала топика
}

# Инициализация Consumer
consumer = Consumer(conf)

# Подписка на топик
topic = 'mssqlserver.dbo.hub_user'
consumer.subscribe([topic])

print("Подключение установлено")

# Чтение сообщений из топика
try:
    while True:
        message = consumer.poll(1.0)  # Ожидание сообщения (1 секунда)
        if message is None:
            continue
        if message.error():
            print(f"Error: {message.error()}")
            continue
        # Вывод сообщения
        print(f"Received message: {message.value().decode('utf-8')}")

except KeyboardInterrupt:
    print("Consumer закрыт")
finally:
    consumer.close()  # Закрытие Consumer
