# Partea 2 – Agent de mesagerie cu gRPC

Broker C# (.NET 8, gRPC) + sender/receiver Python. Contract: `protos/broker.proto` (protobuf).

## Rulare locală (3 terminale)
```
cd Broker && dotnet run                          # broker pe localhost:5000
cd clients && python receiver.py orders r1       # abonat la topicul "orders"
cd clients && python sender.py orders 3          # trimite 3 mesaje OrderCreated (protobuf)
```
Dependențe Python: `pip install grpcio grpcio-tools`.
Regenerare stub-uri după modificarea .proto (din `part2`):
`python -m grpc_tools.protoc -Iprotos --python_out=clients --grpc_python_out=clients protos/broker.proto protos/events.proto`

## Design
- **RPC-uri**: `Publish` (unary), `Subscribe` (server-streaming, brokerul împinge mesajele), `Ack`.
- **Canale**: unidirecționale logic – sender→broker (Publish), broker→receiver (stream Subscribe). Transport: HTTP/2 peste TCP (fiabil, ordonat).
- **Rutare pub/sub** pe *topic*; fiecare abonat primește o copie (fan-out). Fără abonați, mesajele rămân în storage.
- **Validare**: topic (`[A-Za-z0-9._-]`, ≤100), tip cunoscut, octeți decodabili, reguli pe câmpuri (ex. `order_id > 0`), ≤ `MaxPayloadBytes`; mesajele invalide sunt respinse cu `ok=false`, brokerul nu cade.
- **Payload tipizat**: `google.protobuf.Any` care conține un mesaj din `protos/events.proto` (`OrderCreated`, `Notification`); brokerul îl despachetează pentru validare și îl retransmite neschimbat.
- **Concurență**: Kestrel tratează fiecare apel concurent; un `DispatchWorker` (job periodic) livrează din storage; receiverul procesează în ThreadPool.
- **Persistență**: colecții thread-safe + jurnal `store.jsonl` (o linie = un `Message` protobuf în base64, rescris atomic, cu group commit); la repornire mesajele neconfirmate se reîncarcă.
- **Livrare at-least-once**: fără `Ack` în `AckTimeoutMs` → retrimitere; după `MaxAttempts` → topicul `$dlq`. `id` opțional în Publish → deduplicare.
- Limitări față de Partea 1: fără grupuri de consumatori, fără cluster/replicare.
