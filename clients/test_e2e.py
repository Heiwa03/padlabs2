"""End-to-end tests: start the real broker (dotnet) and talk to it over gRPC.

Run from part2/clients:  python test_e2e.py
"""
import os
import subprocess
import threading
import time
import unittest
from concurrent.futures import ThreadPoolExecutor

import grpc
from google.protobuf import any_pb2, wrappers_pb2

import broker_pb2 as pb
import broker_pb2_grpc as rpc
import events_pb2 as ev

HERE = os.path.dirname(os.path.abspath(__file__))
BROKER_DIR = os.path.join(HERE, "..", "Broker")
STORE = os.path.join(BROKER_DIR, "store.jsonl")
ADDR = "localhost:5000"


def pack(msg):
    a = any_pb2.Any()
    a.Pack(msg)
    return a


def order(i=1, **kw):
    return pack(ev.OrderCreated(order_id=i, customer="alice", total=9.5, **kw))


def start_broker():
    env = dict(os.environ, Broker__AckTimeoutMs="600", Broker__MaxAttempts="3", Broker__DispatchIntervalMs="100")
    p = subprocess.Popen(["dotnet", "run", "--no-launch-profile"], cwd=BROKER_DIR, env=env,
                         stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    ch = grpc.insecure_channel(ADDR)
    for _ in range(100):
        try:
            grpc.channel_ready_future(ch).result(timeout=0.3)
            break
        except grpc.FutureTimeoutError:
            time.sleep(0.2)
    return p, ch


def stop_broker(p):
    subprocess.run(["taskkill", "/F", "/T", "/PID", str(p.pid)], capture_output=True)
    subprocess.run(["taskkill", "/F", "/IM", "Broker.exe"], capture_output=True)
    time.sleep(1)


def collect(stub, topic, name, out, ack=True, stop=None):
    """Subscriber thread: appends received messages to `out`."""
    call = stub.Subscribe(pb.SubscribeRequest(topic=topic, subscriber=name))
    stop.append(call)
    try:
        for m in call:
            out.append(m)
            if ack:
                stub.Ack(pb.AckRequest(subscriber=name, message_id=m.id))
    except grpc.RpcError:
        pass


def wait_for(cond, timeout=8):
    end = time.time() + timeout
    while time.time() < end:
        if cond():
            return True
        time.sleep(0.1)
    return cond()


class BrokerTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if os.path.exists(STORE):
            os.remove(STORE)
        cls.proc, cls.ch = start_broker()
        cls.stub = rpc.MessageBrokerStub(cls.ch)

    @classmethod
    def tearDownClass(cls):
        stop_broker(cls.proc)

    def pub(self, topic, payload, **kw):
        return self.stub.Publish(pb.PublishRequest(topic=topic, payload=payload, sender="t", **kw))

    def subscribe(self, topic, name, ack=True):
        out, calls = [], []
        threading.Thread(target=collect, args=(self.stub, topic, name, out, ack, calls), daemon=True).start()
        return out, calls

    def test_01_valid_message_delivered(self):
        out, calls = self.subscribe("t1", "a")
        time.sleep(0.5)
        r = self.pub("t1", order(7))
        self.assertTrue(r.ok)
        self.assertTrue(wait_for(lambda: len(out) == 1))
        got = ev.OrderCreated()
        self.assertTrue(out[0].payload.Is(ev.OrderCreated.DESCRIPTOR))
        out[0].payload.Unpack(got)
        self.assertEqual((got.order_id, got.customer, got.total), (7, "alice", 9.5))
        calls[0].cancel()

    def test_02_payload_not_altered(self):
        sent = pack(ev.Notification(title="disk", body="90% full", severity=ev.Notification.WARNING))
        out, calls = self.subscribe("t2", "a")
        time.sleep(0.5)
        self.assertTrue(self.pub("t2", sent).ok)
        self.assertTrue(wait_for(lambda: len(out) == 1))
        self.assertEqual(out[0].payload.type_url, sent.type_url)
        self.assertEqual(out[0].payload.value, sent.value)      # byte-for-byte identical
        calls[0].cancel()

    def test_03_invalid_messages_rejected_broker_survives(self):
        garbage = any_pb2.Any(type_url="type.googleapis.com/events.OrderCreated", value=bytes([255, 255, 255]))
        unknown = pack(wrappers_pb2.StringValue(value="hi"))
        cases = {
            "empty topic": ("", order()),
            "bad topic": ("bad topic!", order()),
            "no payload": ("t3", None),
            "unknown type": ("t3", unknown),
            "corrupt bytes": ("t3", garbage),
            "order_id = 0": ("t3", order(0)),
            "unspecified severity": ("t3", pack(ev.Notification(title="x"))),
            "too large": ("t3", pack(ev.OrderCreated(order_id=1, customer="x" * 70000))),
            "reserved topic": ("$dlq", order()),
        }
        for name, (topic, payload) in cases.items():
            r = self.stub.Publish(pb.PublishRequest(topic=topic, payload=payload, sender="t"))
            self.assertFalse(r.ok, name)
            self.assertTrue(r.error, name)
        self.assertTrue(self.pub("t3", order()).ok)       # still alive

    def test_04_backlog_when_no_subscriber(self):
        self.assertTrue(self.pub("t4", order(1)).ok)
        self.assertTrue(self.pub("t4", order(2)).ok)
        time.sleep(0.5)
        out, calls = self.subscribe("t4", "late")
        self.assertTrue(wait_for(lambda: len(out) == 2))
        calls[0].cancel()

    def test_05_fanout_to_all_subscribers(self):
        o1, c1 = self.subscribe("t5", "x")
        o2, c2 = self.subscribe("t5", "y")
        time.sleep(0.5)
        self.pub("t5", order())
        self.assertTrue(wait_for(lambda: len(o1) == 1 and len(o2) == 1))
        c1[0].cancel(); c2[0].cancel()

    def test_06_topic_isolation(self):
        o1, c1 = self.subscribe("t6a", "x")
        o2, c2 = self.subscribe("t6b", "y")
        time.sleep(0.5)
        self.pub("t6a", order())
        self.assertTrue(wait_for(lambda: len(o1) == 1))
        time.sleep(0.5)
        self.assertEqual(len(o2), 0)
        c1[0].cancel(); c2[0].cancel()

    def test_07_dedup_by_id(self):
        self.assertFalse(self.pub("t7", order(), id="same").duplicate)
        self.assertTrue(self.pub("t7", order(), id="same").duplicate)

    def test_08_unacked_goes_to_dlq(self):
        dlq, c1 = self.subscribe("$dlq", "dlq")
        noack, c2 = self.subscribe("t8", "lazy", ack=False)
        time.sleep(0.5)
        self.pub("t8", order())
        self.assertTrue(wait_for(lambda: len(dlq) == 1, 10))
        self.assertEqual(len(noack), 3)                # exactly MaxAttempts deliveries
        c1[0].cancel(); c2[0].cancel()

    def test_09_concurrent_publishers(self):
        out, calls = self.subscribe("t9", "a")
        time.sleep(0.5)

        def work(i):
            ch = grpc.insecure_channel(ADDR)
            s = rpc.MessageBrokerStub(ch)
            for j in range(50):
                assert s.Publish(pb.PublishRequest(topic="t9", payload=pack(ev.OrderCreated(order_id=i * 100 + j + 1, customer="c", total=1)))).ok

        with ThreadPoolExecutor(8) as ex:
            list(ex.map(work, range(8)))
        self.assertTrue(wait_for(lambda: len(out) == 400, 15), len(out))
        self.assertEqual(len({m.id for m in out}), 400)
        calls[0].cancel()

    def test_zz_recovery_after_crash(self):
        self.assertTrue(self.pub("t10", order(42), id="persist-1").ok)
        time.sleep(0.5)
        self.assertTrue(os.path.getsize(STORE) > 0)
        stop_broker(self.__class__.proc)               # simulate crash
        self.__class__.proc, self.__class__.ch = start_broker()
        self.__class__.stub = rpc.MessageBrokerStub(self.__class__.ch)
        out, calls = self.subscribe("t10", "after")
        self.assertTrue(wait_for(lambda: len(out) == 1))
        got = ev.OrderCreated()
        out[0].payload.Unpack(got)
        self.assertEqual(got.order_id, 42)
        calls[0].cancel()


if __name__ == "__main__":
    unittest.main(verbosity=2)
