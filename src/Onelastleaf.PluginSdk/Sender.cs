using Grpc.Core;
using Oll.Protocol;

namespace Onelastleaf.PluginSdk;

internal sealed class Sender
{
    private readonly IClientStreamWriter<PluginEnvelope> _stream;
    private readonly SemaphoreSlim _write = new(1, 1);
    private ulong _nextMessageId;
    private string _sessionId = "";
    private string _instanceId = "";

    internal Sender(IClientStreamWriter<PluginEnvelope> stream) => _stream = stream;

    internal string SessionId => _sessionId;
    internal string InstanceId => _instanceId;

    internal void SetIdentity(string sessionId, string instanceId)
        => (_sessionId, _instanceId) = (sessionId, instanceId);

    internal async Task<ulong> SendAsync(
        ulong? replyTo,
        TraceContext trace,
        PluginEnvelope envelope,
        CancellationToken cancellationToken = default,
        Action<ulong>? beforeWrite = null)
    {
        await _write.WaitAsync(cancellationToken);
        try
        {
            _nextMessageId = checked(_nextMessageId + 1);
            var messageId = _nextMessageId;
            beforeWrite?.Invoke(messageId);
            envelope.MessageId = messageId;
            if (replyTo is { } value) envelope.ReplyTo = value;
            envelope.SessionId = _sessionId;
            envelope.PluginInstanceId = _instanceId;
            envelope.Trace = trace.Clone();
            await _stream.WriteAsync(envelope, cancellationToken);
            return messageId;
        }
        finally
        {
            _write.Release();
        }
    }
}
