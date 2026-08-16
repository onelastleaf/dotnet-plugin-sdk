using System.Collections.Concurrent;
using System.Security.Cryptography;
using Google.Protobuf.WellKnownTypes;
using Oll.Protocol;

namespace Onelastleaf.PluginSdk;

public sealed class Host
{
    private readonly Sender _sender;
    private readonly ConcurrentDictionary<ulong, Pending> _pending = new();

    internal Host(
        Sender sender,
        ulong maximumArtifactChunkBytes,
        uint maximumCallDepth,
        uint maximumCausalDepth)
        => (_sender, MaximumArtifactChunkBytes, MaximumCallDepth, MaximumCausalDepth) =
            (sender, maximumArtifactChunkBytes, maximumCallDepth, maximumCausalDepth);

    public ulong MaximumArtifactChunkBytes { get; }
    internal uint MaximumCallDepth { get; }
    internal uint MaximumCausalDepth { get; }

    public async Task<HostCallResponse> CallAsync(
        TraceContext trace,
        HostCallRequest request,
        CancellationToken cancellationToken = default)
    {
        var envelope = await RequestAsync(
            trace,
            new PluginEnvelope { HostCall = request },
            cancellationToken);
        var response = envelope.PayloadCase switch
        {
            PluginEnvelope.PayloadOneofCase.HostResult => envelope.HostResult,
            PluginEnvelope.PayloadOneofCase.ProtocolError => throw new HostProtocolException(envelope.ProtocolError),
            _ => throw new InvalidOperationException("host call received another response kind"),
        };
        if (response.ResultCase == HostCallResponse.ResultOneofCase.Error)
            throw new HostProtocolException(response.Error);
        return response;
    }

    public async Task<GetConfigResponse> GetConfigAsync(
        TraceContext trace,
        ConfigPath? path = null,
        CancellationToken cancellationToken = default)
    {
        var response = await CallAsync(
            trace,
            new HostCallRequest { GetConfig = new GetConfigRequest { Path = path } },
            cancellationToken);
        if (response.ResultCase != HostCallResponse.ResultOneofCase.GetConfig)
            throw new InvalidOperationException("GetConfig received another response kind");
        return response.GetConfig;
    }

    public async Task<InvokeConfigFunctionResponse> InvokeConfigFunctionAsync(
        TraceContext trace,
        ConfigFunctionRef function,
        IEnumerable<ConfigValue> arguments,
        CancellationToken cancellationToken = default)
    {
        var request = new InvokeConfigFunctionRequest { Function = function };
        request.Arguments.AddRange(arguments);
        var response = await CallAsync(
            trace,
            new HostCallRequest { InvokeConfigFunction = request },
            cancellationToken);
        if (response.ResultCase != HostCallResponse.ResultOneofCase.InvokeConfigFunction)
            throw new InvalidOperationException("InvokeConfigFunction received another response kind");
        return response.InvokeConfigFunction;
    }

    public async Task LogAsync(
        TraceContext trace,
        LogLevel level,
        string target,
        string message,
        IReadOnlyDictionary<string, ConfigValue>? fields = null,
        CancellationToken cancellationToken = default)
    {
        var record = new LogRecord
        {
            Timestamp = Timestamp.FromDateTime(DateTime.UtcNow),
            Level = level,
            Target = target,
            Message = message,
        };
        if (fields is not null)
            foreach (var field in fields) record.Fields.Add(field.Key, field.Value);
        await _sender.SendAsync(null, trace, new PluginEnvelope { Log = record }, cancellationToken);
    }

    public async Task<ArtifactStored> StoreArtifactAsync(
        TraceContext trace,
        string jobId,
        ArtifactDescriptor artifact,
        IReadOnlyList<ReadOnlyMemory<byte>> chunks,
        CancellationToken cancellationToken = default)
    {
        if (chunks.Count == 0 || chunks.Any(chunk => chunk.IsEmpty))
            throw new ArgumentException("artifact chunks must be nonempty", nameof(chunks));
        if (chunks.Any(chunk => (ulong)chunk.Length > MaximumArtifactChunkBytes))
            throw new ArgumentException("artifact chunk exceeds the negotiated limit", nameof(chunks));
        ValidateArtifact(artifact, chunks);
        var accepted = await RequestAsync(
            trace,
            new PluginEnvelope
            {
                ArtifactStart = new ArtifactTransferStart
                {
                    JobId = new PluginJobId { Value = jobId },
                    Artifact = artifact,
                    ChunkCount = (uint)chunks.Count,
                },
            },
            cancellationToken);
        if (accepted.PayloadCase != PluginEnvelope.PayloadOneofCase.ArtifactAccepted
            || !Equals(accepted.ArtifactAccepted.ArtifactId, artifact.ArtifactId))
            throw new InvalidOperationException("host did not accept the artifact transfer");
        for (var index = 0; index < chunks.Count; index++)
        {
            await _sender.SendAsync(
                null,
                trace,
                new PluginEnvelope
                {
                    ArtifactChunk = new ArtifactTransferChunk
                    {
                        ArtifactId = artifact.ArtifactId,
                        ChunkIndex = (uint)index,
                        Data = Google.Protobuf.ByteString.CopyFrom(chunks[index].Span),
                    },
                },
                cancellationToken);
        }
        var stored = await RequestAsync(
            trace,
            new PluginEnvelope
            {
                ArtifactComplete = new ArtifactTransferComplete { ArtifactId = artifact.ArtifactId },
            },
            cancellationToken);
        if (stored.PayloadCase != PluginEnvelope.PayloadOneofCase.ArtifactStored
            || !Equals(stored.ArtifactStored.ArtifactId, artifact.ArtifactId))
            throw new InvalidOperationException("host did not store the artifact");
        return stored.ArtifactStored;
    }

    internal void Route(PluginEnvelope envelope)
    {
        if (!_pending.TryRemove(envelope.ReplyTo, out var pending))
            throw new InvalidOperationException("host response names no pending plugin request");
        if (pending.CorrelationId != envelope.Trace.CorrelationId)
            pending.Completion.SetException(new InvalidOperationException("host response changed correlation context"));
        else
            pending.Completion.SetResult(envelope);
    }

    private async Task<PluginEnvelope> RequestAsync(
        TraceContext trace,
        PluginEnvelope envelope,
        CancellationToken cancellationToken)
    {
        var completion = new TaskCompletionSource<PluginEnvelope>(TaskCreationOptions.RunContinuationsAsynchronously);
        ulong messageId = 0;
        using var registration = cancellationToken.Register(() => completion.TrySetCanceled(cancellationToken));
        try
        {
            messageId = await _sender.SendAsync(
                null,
                trace,
                envelope,
                cancellationToken,
                id =>
                {
                    messageId = id;
                    if (!_pending.TryAdd(id, new Pending(trace.CorrelationId, completion)))
                        throw new InvalidOperationException("duplicate pending request ID");
                });
            return await completion.Task;
        }
        finally
        {
            _pending.TryRemove(messageId, out _);
        }
    }

    private sealed record Pending(string CorrelationId, TaskCompletionSource<PluginEnvelope> Completion);

    private static void ValidateArtifact(
        ArtifactDescriptor artifact,
        IReadOnlyList<ReadOnlyMemory<byte>> chunks)
    {
        if (artifact.ArtifactId is null || !Plugin.IsCanonicalUuidV4(artifact.ArtifactId.Value)
            || string.IsNullOrEmpty(artifact.FileName) || string.IsNullOrEmpty(artifact.MediaType)
            || artifact.Sha256.Length != 32)
            throw new ArgumentException("artifact descriptor is invalid", nameof(artifact));

        ulong size = 0;
        using var hash = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
        foreach (var chunk in chunks)
        {
            size = checked(size + (ulong)chunk.Length);
            hash.AppendData(chunk.Span);
        }
        if (size != artifact.SizeBytes
            || !CryptographicOperations.FixedTimeEquals(hash.GetHashAndReset(), artifact.Sha256.Span))
            throw new ArgumentException("artifact size or SHA-256 does not match its bytes", nameof(artifact));
    }
}
