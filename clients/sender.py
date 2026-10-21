"""Sender: serializes a message to JSON and publishes it to a topic via gRPC.

Usage: python sender.py <topic> [count]
       python sender.py orders 5
       python sender.py orders --raw "not json"     (broker must reject it)
"""
import json
import sys
import time

import grpc

import broker_pb2
import broker_pb2_grpc

ADDR = "localhost:5000"


def main():
    topic = sys.argv[1] if len(sys.argv) > 1 else "orders"
    with grpc.insecure_channel(ADDR) as ch:
        stub = broker_pb2_grpc.MessageBrokerStub(ch)

        if len(sys.argv) > 3 and sys.argv[2] == "--raw":
            payloads = [sys.argv[3]]
        else:
            count = int(sys.argv[2]) if len(sys.argv) > 2 else 3
            payloads = [json.dumps({"type": topic, "seq": i, "text": f"hello {i}", "ts": time.time()})
                        for i in range(1, count + 1)]

        for p in payloads:
            try:
                r = stub.Publish(broker_pb2.PublishRequest(topic=topic, payload=p, sender="py-sender"), timeout=5)
                print(f"OK id={r.message_id}" if r.ok else f"REJECTED: {r.error}")
            except grpc.RpcError as e:
                print(f"broker unavailable: {e.code()}")
                return


if __name__ == "__main__":
    main()
