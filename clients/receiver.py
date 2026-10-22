"""Receiver: subscribes to a topic and processes pushed messages concurrently.

Usage: python receiver.py <topic> <name>
"""
import sys
import time
from concurrent.futures import ThreadPoolExecutor

import grpc

import broker_pb2
import broker_pb2_grpc
import events_pb2

ADDR = "localhost:5000"

# type URL -> generated class; the receiver shares the schema with the sender
TYPES = {
    "events.OrderCreated": events_pb2.OrderCreated,
    "events.Notification": events_pb2.Notification,
}


def decode(any_msg):
    cls = TYPES.get(any_msg.type_url.rsplit("/", 1)[-1])
    if cls is None or not any_msg.Is(cls.DESCRIPTOR):
        return None
    obj = cls()
    any_msg.Unpack(obj)
    return obj


def handle(stub, name, msg):
    obj = decode(msg.payload)
    if obj is None:
        print(f"[{name}] unknown payload type {msg.payload.type_url}, skipped", flush=True)
    else:
        fields = ", ".join(f"{f.name}={v}" for f, v in obj.ListFields())
        print(f"[{name}] {msg.topic} <- {msg.sender}: {type(obj).__name__}({fields})", flush=True)
    stub.Ack(broker_pb2.AckRequest(subscriber=name, message_id=msg.id))


def main():
    topic = sys.argv[1] if len(sys.argv) > 1 else "orders"
    name = sys.argv[2] if len(sys.argv) > 2 else "receiver1"
    pool = ThreadPoolExecutor(max_workers=8)   # one task per message, stream never blocked

    while True:   # reconnect if the broker goes down
        try:
            with grpc.insecure_channel(ADDR) as ch:
                stub = broker_pb2_grpc.MessageBrokerStub(ch)
                print(f"[{name}] subscribing to '{topic}'", flush=True)
                for msg in stub.Subscribe(broker_pb2.SubscribeRequest(topic=topic, subscriber=name)):
                    pool.submit(handle, stub, name, msg)
        except grpc.RpcError as e:
            print(f"[{name}] connection lost ({e.code()}), retrying in 2s", flush=True)
            time.sleep(2)


if __name__ == "__main__":
    main()
