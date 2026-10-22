using System.Collections.Concurrent;
using Google.Protobuf;
using System.Threading.Channels;
using Broker.Grpc;
using Grpc.Core;

namespace Broker;

/// Persistent store: every pending (unacknowledged) message is kept in a thread-safe
/// dictionary and mirrored to a file (one base64-encoded protobuf Message per line),
/// so it survives a broker crash.
public class MessageStore
{
    private readonly ConcurrentDictionary<string, Message> _pending = new();
    private readonly SemaphoreSlim _fileLock = new(1, 1);
    private readonly string _path;
    private long _version;          // bumped on every change
    private long _flushedVersion;   // highest version already on disk

    public MessageStore(IConfiguration cfg, ILogger<MessageStore> log)
    {
        _path = cfg["Broker:StorePath"] ?? "store.jsonl";
        if (!File.Exists(_path)) return;
        foreach (var line in File.ReadLines(_path))
        {
            try
            {
                var m = Message.Parser.ParseFrom(Convert.FromBase64String(line));
                _pending[m.Id] = m;
            }
            catch (Exception e) when (e is FormatException or InvalidProtocolBufferException)
            {
                log.LogWarning("Skipping corrupt store line");
            }
        }
        log.LogInformation("Recovered {n} pending messages from {p}", _pending.Count, _path);
    }

    public IEnumerable<Message> Pending => _pending.Values.OrderBy(m => m.TimestampMs);

    public Message? Get(string id) => _pending.TryGetValue(id, out var m) ? m : null;

    public async Task AddAsync(Message m)
    {
        _pending[m.Id] = m;
        await FlushAsync();
    }

    public async Task RemoveAsync(string id)
    {
        if (_pending.TryRemove(id, out _)) await FlushAsync();
    }

    // Group commit: callers queue on the lock; whoever gets it writes the file once for
    // everything changed so far, and the callers behind it find their change already on disk.
    private async Task FlushAsync()
    {
        var mine = Interlocked.Increment(ref _version);
        await _fileLock.WaitAsync();
        try
        {
            if (Volatile.Read(ref _flushedVersion) >= mine) return;
            var covered = Interlocked.Read(ref _version);
            var lines = _pending.Values.Select(m => Convert.ToBase64String(m.ToByteArray()));
            var tmp = _path + ".tmp";
            await File.WriteAllLinesAsync(tmp, lines);
            // On Windows the target can be briefly locked (antivirus/indexer): retry the atomic swap.
            for (var attempt = 1; ; attempt++)
            {
                try { File.Move(tmp, _path, true); break; }
                catch (Exception e) when (e is UnauthorizedAccessException or IOException && attempt < 8)
                {
                    await Task.Delay(25 * attempt);
                }
            }
            Volatile.Write(ref _flushedVersion, covered);
        }
        finally { _fileLock.Release(); }
    }
}

/// Routing layer: topic -> subscribers. Each subscriber owns an unbounded channel.
public class TopicRegistry
{
    public class Subscriber
    {
        public required string Name { get; init; }
        public required string Topic { get; init; }
        public Channel<Message> Queue { get; } = Channel.CreateUnbounded<Message>();
        // per-message delivery state (attempts, last send, acked/dead)
        public ConcurrentDictionary<string, DState> Deliveries { get; } = new();
    }

    public class DState
    {
        public int Attempts;
        public long LastSentMs;
        public bool Acked;
        public bool Dead;
    }

    private readonly ConcurrentDictionary<string, ConcurrentDictionary<string, Subscriber>> _topics = new();

    public Subscriber Add(string topic, string name)
    {
        var s = new Subscriber { Name = name, Topic = topic };
        _topics.GetOrAdd(topic, _ => new()).AddOrUpdate(name, s, (_, old) => { old.Queue.Writer.TryComplete(); return s; });
        return s;
    }

    public void Remove(Subscriber s)
    {
        if (_topics.TryGetValue(s.Topic, out var subs))
            subs.TryRemove(new KeyValuePair<string, Subscriber>(s.Name, s));
        s.Queue.Writer.TryComplete();
    }

    public IEnumerable<Subscriber> All => _topics.Values.SelectMany(v => v.Values);

    public IReadOnlyCollection<Subscriber> For(string topic) =>
        _topics.TryGetValue(topic, out var subs) ? subs.Values.ToArray() : Array.Empty<Subscriber>();
}

/// Background (cron-like) job: periodically pushes stored messages to the live subscribers
/// of their topic. Unacked messages are re-sent after AckTimeoutMs; after MaxAttempts
/// they are copied to the "$dlq" topic. A message with no subscriber stays stored.
public class DispatchWorker(MessageStore store, TopicRegistry registry, IConfiguration cfg, ILogger<DispatchWorker> log) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken ct)
    {
        var tick = TimeSpan.FromMilliseconds(cfg.GetValue("Broker:DispatchIntervalMs", 200));
        var ackTimeout = cfg.GetValue("Broker:AckTimeoutMs", 3000);
        var maxAttempts = cfg.GetValue("Broker:MaxAttempts", 3);
        using var timer = new PeriodicTimer(tick);
        while (await timer.WaitForNextTickAsync(ct))
        {
            try
            {
                var now = Environment.TickCount64;
                foreach (var m in store.Pending)
                {
                    var subs = registry.For(m.Topic);
                    foreach (var sub in subs)
                    {
                        var st = sub.Deliveries.GetOrAdd(m.Id, _ => new TopicRegistry.DState());
                        if (st.Acked || st.Dead) continue;
                        if (st.Attempts > 0 && now - st.LastSentMs < ackTimeout) continue;
                        if (st.Attempts >= maxAttempts)
                        {
                            st.Dead = true;
                            log.LogWarning("{id} not acked by '{s}' after {n} attempts -> $dlq", m.Id, sub.Name, st.Attempts);
                            await store.AddAsync(new Message
                            {
                                Id = Guid.NewGuid().ToString("N"), Topic = "$dlq", Payload = m.Payload,
                                Sender = m.Sender, TimestampMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
                            });
                            continue;
                        }
                        st.Attempts++;
                        st.LastSentMs = now;
                        sub.Queue.Writer.TryWrite(m);
                    }
                    // finished when at least one subscriber exists and all are acked/dead
                    if (subs.Count > 0 && subs.All(x => x.Deliveries.TryGetValue(m.Id, out var d) && (d.Acked || d.Dead)))
                    {
                        await store.RemoveAsync(m.Id);
                        foreach (var x in subs) x.Deliveries.TryRemove(m.Id, out _);
                    }
                }
            }
            catch (Exception e) when (e is not OperationCanceledException)
            {
                log.LogError(e, "dispatch tick failed");
            }
        }
    }
}

public class BrokerService(MessageStore store, TopicRegistry registry, IConfiguration cfg, ILogger<BrokerService> log)
    : MessageBroker.MessageBrokerBase
{
    public override async Task<PublishReply> Publish(PublishRequest req, ServerCallContext ctx)
    {
        var maxBytes = cfg.GetValue("Broker:MaxPayloadBytes", 65536);
        if (string.IsNullOrWhiteSpace(req.Topic))
            return Fail("topic is required");
        if (req.Topic.Length > 100 || req.Topic.Any(c => !(char.IsLetterOrDigit(c) || c is '.' or '-' or '_')))
            return Fail("topic may contain only letters, digits, '.', '-', '_' (max 100)");
        var payloadError = PayloadValidator.Validate(req.Payload, maxBytes);
        if (payloadError != null)
            return Fail(payloadError);

        var id = string.IsNullOrWhiteSpace(req.Id) ? Guid.NewGuid().ToString("N") : req.Id;
        if (id.Length > 128) return Fail("id too long (max 128)");
        if (store.Get(id) != null)
            return new PublishReply { Ok = true, MessageId = id, Duplicate = true };   // idempotent

        var msg = new Message
        {
            Id = id,
            Topic = req.Topic,
            Payload = req.Payload,   // Any forwarded as received (type URL + bytes)
            Sender = req.Sender,
            TimestampMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
        };
        await store.AddAsync(msg);
        log.LogInformation("Stored {id} on '{t}' from '{s}'", msg.Id, msg.Topic, msg.Sender);
        return new PublishReply { Ok = true, MessageId = msg.Id };

        PublishReply Fail(string err)
        {
            log.LogWarning("Rejected message: {e}", err);
            return new PublishReply { Ok = false, Error = err };
        }
    }

    public override async Task Subscribe(SubscribeRequest req, IServerStreamWriter<Message> stream, ServerCallContext ctx)
    {
        if (string.IsNullOrWhiteSpace(req.Topic) || string.IsNullOrWhiteSpace(req.Subscriber))
            throw new RpcException(new Status(StatusCode.InvalidArgument, "topic and subscriber are required"));

        var sub = registry.Add(req.Topic, req.Subscriber);
        log.LogInformation("'{s}' subscribed to '{t}'", req.Subscriber, req.Topic);
        try
        {
            await foreach (var m in sub.Queue.Reader.ReadAllAsync(ctx.CancellationToken))
                await stream.WriteAsync(m);
        }
        catch (OperationCanceledException) { }
        finally
        {
            registry.Remove(sub);
            log.LogInformation("'{s}' unsubscribed from '{t}'", req.Subscriber, req.Topic);
        }
    }

    // Marks the message as processed by this subscriber; the worker deletes it once all have acked.
    public override Task<PublishReply> Ack(AckRequest req, ServerCallContext ctx)
    {
        foreach (var sub in registry.All.Where(x => x.Name == req.Subscriber))
            if (sub.Deliveries.TryGetValue(req.MessageId, out var d)) d.Acked = true;
        return Task.FromResult(new PublishReply { Ok = true, MessageId = req.MessageId });
    }
}
