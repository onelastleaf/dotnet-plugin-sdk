using System.Threading.Channels;
using Grpc.Core;
using Oll.Protocol;

namespace Onelastleaf.PluginSdk.Tests;

internal static class TestTimeout
{
    internal static readonly TimeSpan Value = TimeSpan.FromSeconds(5);
}

internal sealed class TestClientStreamWriter(
    Func<CancellationToken, Task>? beforeWrite = null) : IClientStreamWriter<PluginEnvelope>
{
    private readonly Channel<PluginEnvelope> _messages = Channel.CreateUnbounded<PluginEnvelope>();

    public WriteOptions? WriteOptions { get; set; }

    public Task WriteAsync(PluginEnvelope message)
        => WriteAsync(message, CancellationToken.None);

    public Task CompleteAsync() => Task.CompletedTask;

    public async Task WriteAsync(
        PluginEnvelope message,
        CancellationToken cancellationToken)
    {
        if (beforeWrite is not null)
            await beforeWrite(cancellationToken);
        await _messages.Writer.WriteAsync(message.Clone(), cancellationToken);
    }

    internal Task<PluginEnvelope> ReadAsync()
        => _messages.Reader.ReadAsync().AsTask().WaitAsync(TestTimeout.Value);
}

internal sealed class TestAsyncStreamReader : IAsyncStreamReader<PluginEnvelope>
{
    private readonly Channel<PluginEnvelope> _messages = Channel.CreateUnbounded<PluginEnvelope>();

    public PluginEnvelope Current { get; private set; } = null!;

    public async Task<bool> MoveNext(CancellationToken cancellationToken)
    {
        try
        {
            Current = await _messages.Reader.ReadAsync(cancellationToken);
            return true;
        }
        catch (ChannelClosedException)
        {
            return false;
        }
    }

    internal void Add(PluginEnvelope envelope)
    {
        if (!_messages.Writer.TryWrite(envelope.Clone()))
            throw new InvalidOperationException("test input stream is closed");
    }

    internal void Complete() => _messages.Writer.TryComplete();
}

internal sealed class NonSeekableReadStream(byte[] content) : Stream
{
    private readonly MemoryStream _inner = new(content, writable: false);

    public override bool CanRead => true;
    public override bool CanSeek => false;
    public override bool CanWrite => false;
    public override long Length => throw new NotSupportedException();
    public override long Position
    {
        get => throw new NotSupportedException();
        set => throw new NotSupportedException();
    }

    public override void Flush() => throw new NotSupportedException();
    public override int Read(byte[] buffer, int offset, int count)
        => _inner.Read(buffer, offset, count);
    public override ValueTask<int> ReadAsync(
        Memory<byte> buffer,
        CancellationToken cancellationToken = default)
        => _inner.ReadAsync(buffer, cancellationToken);
    public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
    public override void SetLength(long value) => throw new NotSupportedException();
    public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();

    protected override void Dispose(bool disposing)
    {
        if (disposing)
            _inner.Dispose();
        base.Dispose(disposing);
    }
}

internal sealed class FaultingReadStream(Exception error) : Stream
{
    public override bool CanRead => true;
    public override bool CanSeek => false;
    public override bool CanWrite => false;
    public override long Length => throw new NotSupportedException();
    public override long Position
    {
        get => throw new NotSupportedException();
        set => throw new NotSupportedException();
    }

    public override void Flush() => throw new NotSupportedException();
    public override int Read(byte[] buffer, int offset, int count) => throw error;
    public override ValueTask<int> ReadAsync(
        Memory<byte> buffer,
        CancellationToken cancellationToken = default)
        => ValueTask.FromException<int>(error);
    public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
    public override void SetLength(long value) => throw new NotSupportedException();
    public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();
}

internal static class TestProtocol
{
    internal const string SessionId = "test-session";
    internal const string InstanceId = "test-instance";

    internal static TraceContext Trace(string correlationId = "test-correlation")
        => new()
        {
            CorrelationId = correlationId,
            TaskId = "task",
            TaskGroupId = "group",
        };

    internal static PluginEnvelope HostEnvelope(ulong messageId, string? correlationId = null)
        => new()
        {
            MessageId = messageId,
            SessionId = SessionId,
            PluginInstanceId = InstanceId,
            Trace = Trace(correlationId ?? $"correlation-{messageId}"),
        };
}
