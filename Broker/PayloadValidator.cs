using Broker.Events;
using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;

namespace Broker;

/// Validates the typed payload (google.protobuf.Any). The Any carries a type URL
/// ("type.googleapis.com/events.OrderCreated") plus the serialized bytes; the broker
/// unpacks it with the generated class, so malformed bytes and wrong field values
/// are rejected here instead of reaching receivers.
public static class PayloadValidator
{
    private static readonly Dictionary<string, Func<Any, string?>> Rules = new()
    {
        [OrderCreated.Descriptor.FullName] = any =>
        {
            var o = any.Unpack<OrderCreated>();
            if (o.OrderId <= 0) return "OrderCreated.order_id must be > 0";
            if (string.IsNullOrWhiteSpace(o.Customer)) return "OrderCreated.customer is required";
            if (double.IsNaN(o.Total) || o.Total < 0) return "OrderCreated.total must be >= 0";
            return null;
        },
        [Notification.Descriptor.FullName] = any =>
        {
            var n = any.Unpack<Notification>();
            if (string.IsNullOrWhiteSpace(n.Title)) return "Notification.title is required";
            if (n.Severity == Notification.Types.Severity.Unspecified) return "Notification.severity is required";
            return null;
        },
    };

    public static IEnumerable<string> KnownTypes => Rules.Keys;

    /// Returns null if valid, otherwise a human-readable error.
    public static string? Validate(Any? payload, int maxBytes)
    {
        if (payload == null || string.IsNullOrEmpty(payload.TypeUrl)) return "payload is required";
        if (payload.CalculateSize() > maxBytes) return $"payload exceeds {maxBytes} bytes";

        var typeName = Any.GetTypeName(payload.TypeUrl);
        if (!Rules.TryGetValue(typeName, out var rule))
            return $"unknown payload type '{typeName}' (known: {string.Join(", ", Rules.Keys)})";
        try { return rule(payload); }
        catch (InvalidProtocolBufferException e) { return $"payload bytes are not a valid {typeName}: {e.Message}"; }
    }
}
