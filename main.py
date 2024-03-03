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


@app.get("/kafka-consume")
async def consume():
    print("consumer started!")
    consumer = AIOKafkaConsumer("foobar", "foobar1", bootstrap_servers='localhost:9092', group_id="123")
    await consumer.start()
    print("consumer.start code is started!")
    try:
        # cousumer 가 특정 토픽에 데이터가 들어오기전까지 무한정 대기하는 현상
        async for msg in consumer:
            print("consumed: ", msg.topic, msg.partition, msg.offset, msg.key, msg.value, msg.timestamp)
    except Exception as e:
        print(e)
        return {"response": e}
    return {"response": "consume success from kafka"}


@app.get("/kafka-consume")
async def consume():
    print("consumer started!")
    consumer = AIOKafkaConsumer("foobar", "foobar1", bootstrap_servers='localhost:9092', group_id="123",
                                request_timeout_ms=40000)
    await consumer.start()
    print("consumer.start code is started!")
    try:
        # timeout 시간동안 대기후 wake up
        msg = await asyncio.wait_for(consumer.getone(),timeout=5)
        print(msg.topic, msg.offset, msg.key, msg.value)
    except Exception as e:
        print(e)
    finally:
        await consumer.stop()
        return {"response": "consume success from kafka"}
