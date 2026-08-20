using Grpc.Core;
using Oll.Protocol;

namespace Onelastleaf.PluginSdk;

internal sealed class Sender : IDisposable
{
    // A stalled transport must not turn concurrent plugin work into an unbounded
    // collection of SemaphoreSlim waiters.
    internal const int MaximumQueuedSends = 256;

    private readonly IClientStreamWriter<PluginEnvelope> _stream;
    private readonly SemaphoreSlim _write = new(1, 1);
    private ulong _nextMessageId;
    private string _sessionId = "";
    private string _instanceId = "";
    private int _queuedSends;
    private int _disposed;
    private int _semaphoreDisposed;

    internal Sender(IClientStreamWriter<PluginEnvelope> stream)
        => _stream = stream ?? throw new ArgumentNullException(nameof(stream));

    internal string SessionId => _sessionId;
    internal string InstanceId => _instanceId;

    internal void SetIdentity(string sessionId, string instanceId)
    {
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);
        if (string.IsNullOrEmpty(sessionId) || string.IsNullOrEmpty(instanceId))
            throw new ArgumentException("session and plugin-instance identities must not be empty");
        if (_sessionId.Length != 0 || _instanceId.Length != 0)
            throw new InvalidOperationException("sender identity is already set");
        (_sessionId, _instanceId) = (sessionId, instanceId);
    }

    internal async Task<ulong> SendAsync(
        ulong? replyTo,
        TraceContext trace,
        PluginEnvelope envelope,
        Action<ulong>? beforeWrite = null,
        CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);
        ArgumentNullException.ThrowIfNull(trace);
        ArgumentNullException.ThrowIfNull(envelope);

        if (Interlocked.Increment(ref _queuedSends) > MaximumQueuedSends)
        {
            Interlocked.Decrement(ref _queuedSends);
            throw new InvalidOperationException(
                $"plugin send queue exceeded its {MaximumQueuedSends}-message limit");
        }

        var entered = false;
        try
        {
            await _write.WaitAsync(cancellationToken).ConfigureAwait(false);
            entered = true;
            ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);

            _nextMessageId = checked(_nextMessageId + 1);
            var messageId = _nextMessageId;
            beforeWrite?.Invoke(messageId);
            envelope.MessageId = messageId;
            if (replyTo is { } value)
                envelope.ReplyTo = value;
            envelope.SessionId = _sessionId;
            envelope.PluginInstanceId = _instanceId;
            envelope.Trace = trace.Clone();
            await _stream.WriteAsync(envelope, cancellationToken).ConfigureAwait(false);
            return messageId;
        }
        finally
        {
            if (entered)
                _write.Release();
            Interlocked.Decrement(ref _queuedSends);
            TryDisposeSemaphore();
        }
    }

    public void Dispose()
    {
        if (Interlocked.Exchange(ref _disposed, 1) == 0)
            TryDisposeSemaphore();
    }

    private void TryDisposeSemaphore()
    {
        if (Volatile.Read(ref _disposed) != 0
            && Volatile.Read(ref _queuedSends) == 0
            && Interlocked.Exchange(ref _semaphoreDisposed, 1) == 0)
            _write.Dispose();
    }
}
