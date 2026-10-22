"""Sender: builds a typed protobuf message, wraps it in Any and publishes it to a topic.

Usage: python sender.py <topic> [count] [--type order|notification] [--invalid]
       python sender.py orders 5
       python sender.py alerts 2 --type notification
       python sender.py orders 1 --invalid     (order_id = 0, the broker must reject it)
"""
import argparse

import grpc
from google.protobuf import any_pb2

import broker_pb2
import broker_pb2_grpc
import events_pb2

ADDR = "localhost:5000"


def make_payload(kind, i, invalid):
    """Return a typed message; the sender serializes it (Any.Pack -> type URL + bytes)."""
    if kind == "notification":
        msg = events_pb2.Notification(
            title=f"alert {i}", body="something happened",
            severity=events_pb2.Notification.SEVERITY_UNSPECIFIED if invalid else events_pb2.Notification.WARNING)
    else:
        msg = events_pb2.OrderCreated(order_id=0 if invalid else i, customer="alice", total=10.5 * i)
    any_msg = any_pb2.Any()
    any_msg.Pack(msg)
    return any_msg


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("topic", nargs="?", default="orders")
    ap.add_argument("count", nargs="?", type=int, default=3)
    ap.add_argument("--type", choices=["order", "notification"], default="order")
    ap.add_argument("--invalid", action="store_true")
    a = ap.parse_args()

    with grpc.insecure_channel(ADDR) as ch:
        stub = broker_pb2_grpc.MessageBrokerStub(ch)
        for i in range(1, a.count + 1):
            req = broker_pb2.PublishRequest(topic=a.topic, payload=make_payload(a.type, i, a.invalid), sender="py-sender")
            try:
                r = stub.Publish(req, timeout=5)
                print(f"OK id={r.message_id}" if r.ok else f"REJECTED: {r.error}")
            except grpc.RpcError as e:
                print(f"broker unavailable: {e.code()}")
                return


if __name__ == "__main__":
    main()
