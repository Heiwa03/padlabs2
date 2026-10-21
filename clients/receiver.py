"""Receiver: subscribes to a topic and processes pushed messages concurrently.

Usage: python receiver.py <topic> <name>
"""
import json
import sys
import time
from concurrent.futures import ThreadPoolExecutor

import grpc

import broker_pb2
import broker_pb2_grpc

ADDR = "localhost:5000"


def handle(stub, name, msg):
    try:
        data = json.loads(msg.payload)
        print(f"[{name}] {msg.topic} <- {msg.sender}: {data}", flush=True)
    except json.JSONDecodeError:
        print(f"[{name}] invalid JSON dropped: {msg.id}", flush=True)
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
