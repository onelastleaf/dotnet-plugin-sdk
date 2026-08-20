using System.Buffers;
using System.Security.Cryptography;
using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;
using Oll.Protocol;

namespace Onelastleaf.PluginSdk;

internal sealed class Host
{
    internal const int DefaultArtifactChunkBytes = 64 * 1024;

    private readonly Sender _sender;
    private readonly TimeProvider _timeProvider;
    private readonly object _gate = new();
    private readonly Dictionary<ulong, PendingRequest> _pending = [];
    private Exception? _closedError;

    internal Host(
        Sender sender,
        ulong maximumArtifactChunkBytes,
        uint maximumCallDepth,
        uint maximumCausalDepth,
        TimeProvider? timeProvider = null)
    {
        _sender = sender ?? throw new ArgumentNullException(nameof(sender));
        ArgumentOutOfRangeException.ThrowIfZero(maximumArtifactChunkBytes);
        MaximumArtifactChunkBytes = maximumArtifactChunkBytes;
        MaximumCallDepth = maximumCallDepth;
        MaximumCausalDepth = maximumCausalDepth;
        _timeProvider = timeProvider ?? TimeProvider.System;
    }

    internal ulong MaximumArtifactChunkBytes { get; }
    internal uint MaximumCallDepth { get; }
    internal uint MaximumCausalDepth { get; }

    internal async Task<HostCallResponse> CallAsync(
        TraceContext trace,
        HostCallRequest request,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        var envelope = await RequestAsync(
            trace,
            new PluginEnvelope { HostCall = request.Clone() },
            cancellationToken).ConfigureAwait(false);
        var response = envelope.PayloadCase switch
        {
            PluginEnvelope.PayloadOneofCase.HostResult => envelope.HostResult,
            PluginEnvelope.PayloadOneofCase.ProtocolError => throw new HostProtocolException(envelope.ProtocolError),
            _ => throw UnexpectedResponse("host call", envelope),
        };
        if (response.ResultCase == HostCallResponse.ResultOneofCase.Error)
            throw new HostProtocolException(response.Error);
        return response.Clone();
    }

    internal async Task<GetConfigResponse> GetConfigAsync(
        TraceContext trace,
        ConfigPath? path = null,
        CancellationToken cancellationToken = default)
    {
        var response = await CallAsync(
            trace,
            new HostCallRequest
            {
                GetConfig = new GetConfigRequest { Path = path?.Clone() },
            },
            cancellationToken).ConfigureAwait(false);
        if (response.ResultCase != HostCallResponse.ResultOneofCase.GetConfig)
            throw new InvalidDataException("GetConfig received another host-call result kind");
        return response.GetConfig.Clone();
    }

    internal async Task<InvokeConfigFunctionResponse> InvokeConfigFunctionAsync(
        TraceContext trace,
        ConfigFunctionRef function,
        IEnumerable<ConfigValue> arguments,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(function);
        ArgumentNullException.ThrowIfNull(arguments);
        var request = new InvokeConfigFunctionRequest { Function = function.Clone() };
        request.Arguments.AddRange(arguments.Select(static argument =>
            argument?.Clone() ?? throw new ArgumentException("arguments must not contain null")));
        var response = await CallAsync(
            trace,
            new HostCallRequest { InvokeConfigFunction = request },
            cancellationToken).ConfigureAwait(false);
        if (response.ResultCase != HostCallResponse.ResultOneofCase.InvokeConfigFunction)
            throw new InvalidDataException("InvokeConfigFunction received another host-call result kind");
        return response.InvokeConfigFunction.Clone();
    }

    internal Task LogAsync(
        TraceContext trace,
        LogLevel level,
        string target,
        string message,
        IReadOnlyDictionary<string, ConfigValue>? fields = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(target);
        ArgumentNullException.ThrowIfNull(message);
        EnsureOpen();
        var record = new LogRecord
        {
            Timestamp = Timestamp.FromDateTimeOffset(_timeProvider.GetUtcNow()),
            Level = level,
            Target = target,
            Message = message,
        };
        if (fields is not null)
        {
            foreach (var (key, value) in fields)
            {
                ArgumentException.ThrowIfNullOrEmpty(key);
                record.Fields.Add(
                    key,
                    value?.Clone() ?? throw new ArgumentException("log fields must not contain null", nameof(fields)));
            }
        }
        return SendOneWayAsync(trace, new PluginEnvelope { Log = record }, cancellationToken);
    }

    internal async Task<StoredArtifact> StoreArtifactAsync(
        TraceContext trace,
        string jobId,
        ArtifactDescriptor artifact,
        Stream content,
        int? chunkSize = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(trace);
        ArgumentException.ThrowIfNullOrEmpty(jobId);
        ArgumentNullException.ThrowIfNull(artifact);
        ArgumentNullException.ThrowIfNull(content);
        if (!content.CanRead || !content.CanSeek)
            throw new ArgumentException(
                "artifact content must be a readable, seekable stream so it can be verified before transfer",
                nameof(content));

        var descriptor = artifact.Clone();
        ValidateArtifactDescriptor(descriptor);
        var defaultChunkSize = (int)Math.Min(
            MaximumArtifactChunkBytes,
            (ulong)DefaultArtifactChunkBytes);
        var effectiveChunkSize = ValidateChunkSize(chunkSize ?? defaultChunkSize);
        var startingPosition = content.Position;
        var length = content.Length;
        if (startingPosition < 0
            || length < startingPosition
            || (ulong)(length - startingPosition) != descriptor.SizeBytes)
            throw new ArgumentException("artifact size does not match the remaining stream length", nameof(artifact));

        await VerifyArtifactHashAsync(
            content,
            startingPosition,
            descriptor.Sha256,
            effectiveChunkSize,
            cancellationToken).ConfigureAwait(false);

        var chunkCount = GetChunkCount(descriptor.SizeBytes, effectiveChunkSize);
        var accepted = await RequestAsync(
            trace,
            new PluginEnvelope
            {
                ArtifactStart = new ArtifactTransferStart
                {
                    JobId = new PluginJobId { Value = jobId },
                    Artifact = descriptor.Clone(),
                    ChunkCount = chunkCount,
                },
            },
            cancellationToken).ConfigureAwait(false);
        ThrowIfProtocolError(accepted);
        if (accepted.PayloadCase != PluginEnvelope.PayloadOneofCase.ArtifactAccepted
            || !Equals(accepted.ArtifactAccepted.ArtifactId, descriptor.ArtifactId))
            throw UnexpectedResponse("artifact start", accepted);

        await SendArtifactChunksAsync(
            trace,
            descriptor,
            content,
            startingPosition,
            effectiveChunkSize,
            chunkCount,
            cancellationToken).ConfigureAwait(false);

        var stored = await RequestAsync(
            trace,
            new PluginEnvelope
            {
                ArtifactComplete = new ArtifactTransferComplete
                {
                    ArtifactId = descriptor.ArtifactId.Clone(),
                },
            },
            cancellationToken).ConfigureAwait(false);
        ThrowIfProtocolError(stored);
        if (stored.PayloadCase != PluginEnvelope.PayloadOneofCase.ArtifactStored
            || !Equals(stored.ArtifactStored.ArtifactId, descriptor.ArtifactId))
            throw UnexpectedResponse("artifact completion", stored);
        return new StoredArtifact(descriptor);
    }

    internal void Route(PluginEnvelope envelope)
    {
        ArgumentNullException.ThrowIfNull(envelope);
        PendingRequest pending;
        lock (_gate)
        {
            if (!_pending.Remove(envelope.ReplyTo, out pending!))
                throw new InvalidDataException("host response names no pending plugin request");
        }

        // A request that reached the wire remains routable after its caller
        // cancels. Its one matching late reply is consumed, not mistaken for a
        // response to an unknown request.
        if (!pending.Trace.Equals(envelope.Trace))
        {
            var error = new InvalidDataException("host response changed its trace context");
            if (!pending.IsAbandoned)
                pending.Completion.TrySetException(error);
            throw error;
        }
        if (pending.IsAbandoned)
            return;
        pending.Completion.TrySetResult(envelope);
    }

    internal void Close(Exception? error = null)
    {
        PendingRequest[] pending;
        var closed = error ?? new IOException("plugin session closed");
        lock (_gate)
        {
            if (_closedError is not null)
                return;
            _closedError = closed;
            pending = _pending.Values.ToArray();
            _pending.Clear();
        }
        foreach (var request in pending)
        {
            if (!request.IsAbandoned)
                request.Completion.TrySetException(closed);
        }
    }

    private async Task<PluginEnvelope> RequestAsync(
        TraceContext trace,
        PluginEnvelope envelope,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(trace);
        cancellationToken.ThrowIfCancellationRequested();
        var pending = new PendingRequest(trace.Clone());
        ulong messageId = 0;
        try
        {
            messageId = await _sender.SendAsync(
                null,
                trace,
                envelope,
                id =>
                {
                    messageId = id;
                    Register(id, pending);
                },
                cancellationToken).ConfigureAwait(false);
        }
        catch
        {
            // A failed WriteAsync did not admit the message to the stream. This
            // differs from caller cancellation after a successful write, where
            // the pending entry must remain as a late-response tombstone.
            Unregister(messageId, pending);
            throw;
        }

        try
        {
            return await pending.Completion.Task.WaitAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            Abandon(messageId, pending);
            throw;
        }
    }

    private void Register(ulong messageId, PendingRequest pending)
    {
        lock (_gate)
        {
            if (_closedError is not null)
                throw new InvalidOperationException("plugin session is closed", _closedError);
            if (!_pending.TryAdd(messageId, pending))
                throw new InvalidOperationException("duplicate pending request ID");
        }
    }

    private void Abandon(ulong messageId, PendingRequest pending)
    {
        lock (_gate)
        {
            if (messageId != 0
                && _pending.TryGetValue(messageId, out var current)
                && ReferenceEquals(current, pending))
                pending.IsAbandoned = true;
        }
    }

    private void Unregister(ulong messageId, PendingRequest pending)
    {
        lock (_gate)
        {
            if (messageId != 0
                && _pending.TryGetValue(messageId, out var current)
                && ReferenceEquals(current, pending))
                _pending.Remove(messageId);
        }
    }

    private void EnsureOpen()
    {
        lock (_gate)
        {
            if (_closedError is not null)
                throw new InvalidOperationException("plugin session is closed", _closedError);
        }
    }

    private async Task SendOneWayAsync(
        TraceContext trace,
        PluginEnvelope envelope,
        CancellationToken cancellationToken)
    {
        await _sender.SendAsync(null, trace, envelope, cancellationToken: cancellationToken)
            .ConfigureAwait(false);
    }

    private int ValidateChunkSize(int chunkSize)
    {
        if (chunkSize <= 0 || (ulong)chunkSize > MaximumArtifactChunkBytes)
            throw new ArgumentOutOfRangeException(
                nameof(chunkSize),
                $"artifact chunk size must be between 1 and {MaximumArtifactChunkBytes} bytes");
        return chunkSize;
    }

    private static uint GetChunkCount(ulong sizeBytes, int chunkSize)
    {
        if (sizeBytes == 0)
            return 0;
        var count = checked(((sizeBytes - 1) / (ulong)chunkSize) + 1);
        if (count > uint.MaxValue)
            throw new ArgumentException("artifact requires more than UInt32.MaxValue chunks");
        return (uint)count;
    }

    private static async Task VerifyArtifactHashAsync(
        Stream content,
        long startingPosition,
        ByteString expectedHash,
        int bufferSize,
        CancellationToken cancellationToken)
    {
        var buffer = ArrayPool<byte>.Shared.Rent(bufferSize);
        try
        {
            content.Position = startingPosition;
            using var hash = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
            while (true)
            {
                var read = await content.ReadAsync(
                    buffer.AsMemory(0, bufferSize),
                    cancellationToken).ConfigureAwait(false);
                if (read == 0)
                    break;
                hash.AppendData(buffer.AsSpan(0, read));
            }
            if (!CryptographicOperations.FixedTimeEquals(hash.GetHashAndReset(), expectedHash.Span))
                throw new ArgumentException("artifact SHA-256 does not match its content", nameof(expectedHash));
        }
        finally
        {
            content.Position = startingPosition;
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }

    private async Task SendArtifactChunksAsync(
        TraceContext trace,
        ArtifactDescriptor descriptor,
        Stream content,
        long startingPosition,
        int chunkSize,
        uint chunkCount,
        CancellationToken cancellationToken)
    {
        if (chunkCount == 0)
            return;
        var buffer = ArrayPool<byte>.Shared.Rent(chunkSize);
        try
        {
            content.Position = startingPosition;
            ulong remaining = descriptor.SizeBytes;
            for (uint index = 0; index < chunkCount; index++)
            {
                var expected = (int)Math.Min((ulong)chunkSize, remaining);
                await content.ReadExactlyAsync(
                    buffer.AsMemory(0, expected),
                    cancellationToken).ConfigureAwait(false);
                await _sender.SendAsync(
                    null,
                    trace,
                    new PluginEnvelope
                    {
                        ArtifactChunk = new ArtifactTransferChunk
                        {
                            ArtifactId = descriptor.ArtifactId.Clone(),
                            ChunkIndex = index,
                            Data = ByteString.CopyFrom(buffer.AsSpan(0, expected)),
                        },
                    },
                    cancellationToken: cancellationToken).ConfigureAwait(false);
                remaining -= (ulong)expected;
            }
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }

    private static void ValidateArtifactDescriptor(ArtifactDescriptor artifact)
    {
        if (artifact.ArtifactId is null
            || !ProtocolValidation.IsCanonicalUuidV4(artifact.ArtifactId.Value)
            || string.IsNullOrEmpty(artifact.FileName)
            || string.IsNullOrEmpty(artifact.MediaType)
            || artifact.Sha256.Length != 32)
            throw new ArgumentException("artifact descriptor is invalid", nameof(artifact));
    }

    private static void ThrowIfProtocolError(PluginEnvelope envelope)
    {
        if (envelope.PayloadCase == PluginEnvelope.PayloadOneofCase.ProtocolError)
            throw new HostProtocolException(envelope.ProtocolError);
    }

    private static InvalidDataException UnexpectedResponse(string operation, PluginEnvelope envelope)
        => new($"{operation} received unexpected {envelope.PayloadCase} response");

    private sealed class PendingRequest(TraceContext trace)
    {
        internal TraceContext Trace { get; } = trace;
        internal TaskCompletionSource<PluginEnvelope> Completion { get; } =
            new(TaskCreationOptions.RunContinuationsAsynchronously);
        internal bool IsAbandoned { get; set; }
    }
}
