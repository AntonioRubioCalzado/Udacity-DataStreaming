import asyncio
from confluent_kafka import Consumer

async def consume_function(topic, broker_url, group_id):
    """
    Function that creates a kafka consumer and subscribes it to a topic,
    """
    cons = Consumer({'bootstrap.servers': broker_url,
                     'group.id': group_id})
    
    cons.subscribe([topic])
    
    while True:
        messages = cons.consume(10, timeout=0.5)
        for m in messages:
            if m is None:
                print("Message is Empty")
            elif m.error() is not None:
                print(f"Error: {m.error()}")
            else:
                print(f"Key:{m.key()}; Value: {m.value()}")
            
            await asyncio.sleep(0.05)
    

def main():
    try:
        TOPIC = "com.udacity.police.calls"
        BROKER_URL = "localhost:9092"
        GROUP_ID = '0'
        
        asyncio.run(consume_function(TOPIC, BROKER_URL, GROUP_ID))
    except Exception as e:
        print (f"Exception: {e}")

if __name__ == "__main__":
    main()