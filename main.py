from aiokafka import AIOKafkaProducer, AIOKafkaConsumer

app = FastAPI()

@app.post("/kafka-produce")
async def produce(text: str):
    producer = AIOKafkaProducer(bootstrap_servers='localhost:9092')
    await producer.start()
    try:
        for _ in range(3):
            await producer.send("foobar", bytes(text, 'utf-8'))
            await producer.flush()
            print("send succefuly t0 kafka : ", text)
    except Exception as e:
        print(e)
    await producer.stop()
    return {"response": f"send synchronously {text}"}


@app.get("/kafka-consumer")
async def consume():
    print("consumer started!")
    consumer = AIOKafkaConsumer("foobar", "foobar1", bootstrap_servers='localhost:9092', group_id="123")
    await consumer.start()
    print("consumer.start code is started!")
    try:
        async for msg in consumer:
            print("consumed: ", msg.topic, msg.partition, msg.offset, msg.key, msg.value, msg.timestamp)
    except Exception as e:
        print(e)
        return {"response": e}
    return {"response": "consume success from kafka"}
