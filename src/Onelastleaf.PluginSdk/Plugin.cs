using System.Net;
using System.Text.RegularExpressions;
using Grpc.Core;
using Grpc.Net.Client;
using Oll.Protocol;

namespace Onelastleaf.PluginSdk;

public sealed class Plugin
{
    private const int MaximumEnvelopeBytes = 64 * 1024 * 1024;
    private readonly Dictionary<string, RegisteredAction> _actions = [];

    private Plugin(string id, string version) => (Id, Version) = (id, version);

    public string Id { get; }
    public string Version { get; }

    public static Plugin Create(string id, string version)
    {
        ValidatePluginId(id);
        if (string.IsNullOrEmpty(version)) throw new ArgumentException("plugin version must not be empty", nameof(version));
        return new Plugin(id, version);
    }

    public Plugin Action(
        string name,
        string description,
        Func<ActionContext, IReadOnlyList<string>, Task<ActionResult>> handler)
    {
        if (string.IsNullOrEmpty(name) || handler is null)
            throw new ArgumentException("action name and handler are required");
        if (!_actions.TryAdd(name, new RegisteredAction(description, handler)))
            throw new ArgumentException($"duplicate action {name}", nameof(name));
        return this;
    }

    public Task RunAsync(CancellationToken cancellationToken = default)
    {
        var endpoint = Environment.GetEnvironmentVariable("OLL_PLUGIN_ENDPOINT")
            ?? throw new InvalidOperationException("OLL_PLUGIN_ENDPOINT is required");
        return RunAtAsync(endpoint, Console.OpenStandardInput(), cancellationToken);
    }

    internal async Task RunAtAsync(string endpoint, Stream parentLiveness, CancellationToken cancellationToken)
    {
        var uri = ValidateEndpoint(endpoint);
        using var sessionCancellation = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        var parentEof = ConsumeUntilEofAsync(parentLiveness, cancellationToken);
        _ = parentEof.ContinueWith(
            _ => sessionCancellation.Cancel(),
            CancellationToken.None,
            TaskContinuationOptions.ExecuteSynchronously,
            TaskScheduler.Default);
        cancellationToken = sessionCancellation.Token;
        try
        {
            using var channel = GrpcChannel.ForAddress(uri, new GrpcChannelOptions
            {
                MaxReceiveMessageSize = MaximumEnvelopeBytes,
                MaxSendMessageSize = MaximumEnvelopeBytes,
            });
            var client = new PluginRuntime.PluginRuntimeClient(channel);
            using var stream = client.Connect(cancellationToken: cancellationToken);
            var sender = new Sender(stream.RequestStream);
            ulong lastHostMessageId = 0;
            var first = await ReceiveAsync(
                stream.ResponseStream,
                sender: null,
                lastHostMessageId,
                uint.MaxValue,
                uint.MaxValue,
                cancellationToken);
            lastHostMessageId = first.MessageId;
            if (first.HasReplyTo || first.PayloadCase != PluginEnvelope.PayloadOneofCase.HostHello)
                throw new InvalidOperationException("HostHello must be the first host message");
            if (first.SessionId.Length == 0 || first.PluginInstanceId.Length == 0)
                throw new InvalidOperationException("HostHello envelope omitted its session or instance identity");
            ValidateHello(first.HostHello);
            ValidateTrace(first.Trace, first.HostHello.MaximumCallDepth, first.HostHello.MaximumCausalDepth);
            sender.SetIdentity(first.SessionId, first.PluginInstanceId);
            var hello = new PluginHello
            {
                PluginId = new PluginId { Value = Id },
                PluginName = first.HostHello.PluginName,
                PluginVersion = Version,
            };
            hello.Actions.AddRange(_actions.Select(action => new ActionDescriptor
            {
                Name = action.Key,
                Description = action.Value.Description,
            }));
            await sender.SendAsync(null, first.Trace, new PluginEnvelope { PluginHello = hello }, cancellationToken);
            var ready = await ReceiveAsync(
                stream.ResponseStream,
                sender,
                lastHostMessageId,
                first.HostHello.MaximumCallDepth,
                first.HostHello.MaximumCausalDepth,
                cancellationToken);
            lastHostMessageId = ready.MessageId;
            if (ready.HasReplyTo || ready.PayloadCase != PluginEnvelope.PayloadOneofCase.Ready
                || ready.Trace.CorrelationId != first.Trace.CorrelationId)
                throw new InvalidOperationException("host SessionReady must follow PluginHello");
            await sender.SendAsync(null, first.Trace, new PluginEnvelope { Ready = new SessionReady() }, cancellationToken);
            var host = new Host(
                sender,
                first.HostHello.MaximumArtifactChunkBytes,
                first.HostHello.MaximumCallDepth,
                first.HostHello.MaximumCausalDepth);
            await ServeAsync(stream.ResponseStream, sender, host, lastHostMessageId, cancellationToken);
            await stream.RequestStream.CompleteAsync();
        }
        catch (OperationCanceledException) when (parentEof.IsCompletedSuccessfully)
        {
        }
        catch (RpcException error) when (
            parentEof.IsCompletedSuccessfully && error.StatusCode == StatusCode.Cancelled)
        {
        }
    }

    private async Task ServeAsync(
        IAsyncStreamReader<PluginEnvelope> incoming,
        Sender sender,
        Host host,
        ulong lastHostMessageId,
        CancellationToken cancellationToken)
    {
        var jobs = new System.Collections.Concurrent.ConcurrentDictionary<string, ActiveJob>();
        while (true)
        {
            var envelope = await ReadAsync(incoming, cancellationToken);
            ValidateEnvelope(envelope, sender, lastHostMessageId);
            ValidateTrace(envelope.Trace, host.MaximumCallDepth, host.MaximumCausalDepth);
            lastHostMessageId = envelope.MessageId;
            if (envelope.HasReplyTo)
            {
                host.Route(envelope);
                continue;
            }
            switch (envelope.PayloadCase)
            {
                case PluginEnvelope.PayloadOneofCase.StartJob:
                    await StartJobAsync(envelope, sender, host, jobs, cancellationToken);
                    break;
                case PluginEnvelope.PayloadOneofCase.CancelJob:
                    var jobId = envelope.CancelJob.JobId?.Value ?? "";
                    if (!jobs.TryRemove(jobId, out var job))
                        throw new InvalidOperationException("cancellation names no active job");
                    job.Cancellation.Cancel();
                    try { await job.Task; } catch (OperationCanceledException) { }
                    await sender.SendAsync(
                        envelope.MessageId,
                        envelope.Trace,
                        new PluginEnvelope
                        {
                            CancelJobAcknowledged = new CancelJobAcknowledged { JobId = envelope.CancelJob.JobId },
                        },
                        cancellationToken);
                    break;
                case PluginEnvelope.PayloadOneofCase.Heartbeat:
                    await sender.SendAsync(envelope.MessageId, envelope.Trace, new PluginEnvelope { Heartbeat = envelope.Heartbeat }, cancellationToken);
                    break;
                case PluginEnvelope.PayloadOneofCase.Shutdown:
                    foreach (var active in jobs.Values) active.Cancellation.Cancel();
                    await Task.WhenAll(jobs.Values.Select(active => active.Task).Select(IgnoreCancellation));
                    await sender.SendAsync(envelope.MessageId, envelope.Trace, new PluginEnvelope { ShutdownAcknowledged = new ShutdownAcknowledged() }, cancellationToken);
                    return;
                case PluginEnvelope.PayloadOneofCase.ProtocolError:
                    throw new HostProtocolException(envelope.ProtocolError);
                default:
                    throw new InvalidOperationException("unexpected host-initiated message");
            }
        }
    }

    private async Task StartJobAsync(
        PluginEnvelope envelope,
        Sender sender,
        Host host,
        System.Collections.Concurrent.ConcurrentDictionary<string, ActiveJob> jobs,
        CancellationToken sessionCancellation)
    {
        var request = envelope.StartJob;
        var id = request.JobId?.Value ?? "";
        if (!IsCanonicalUuidV4(id) || jobs.ContainsKey(id)
            || request.InvocationCase != StartJobRequest.InvocationOneofCase.Action)
            throw new InvalidOperationException("invalid StartJobRequest");
        if (!_actions.TryGetValue(request.Action.Action, out var action))
            throw new InvalidOperationException($"unknown action {request.Action.Action}");
        await sender.SendAsync(envelope.MessageId, envelope.Trace, new PluginEnvelope { JobAccepted = new JobAccepted { JobId = request.JobId } }, sessionCancellation);
        var cancellation = CancellationTokenSource.CreateLinkedTokenSource(sessionCancellation);
        var admitted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var task = Task.Run(async () =>
        {
            await admitted.Task;
            try
            {
                var context = new ActionContext(
                    id,
                    request.Deadline,
                    envelope.Trace,
                    envelope.MessageId,
                    host,
                    cancellation.Token);
                var result = await action.Handler(context, request.Action.Arguments);
                cancellation.Token.ThrowIfCancellationRequested();
                var update = new JobUpdate
                {
                    JobId = request.JobId,
                    State = JobState.Succeeded,
                    Progress = 1,
                    Result = result.Result,
                };
                update.Artifacts.AddRange(result.Artifacts ?? []);
                await sender.SendAsync(null, envelope.Trace, new PluginEnvelope { JobUpdate = update }, sessionCancellation);
            }
            catch (OperationCanceledException) when (cancellation.IsCancellationRequested) { }
            catch (Exception error)
            {
                await sender.SendAsync(null, envelope.Trace, new PluginEnvelope
                {
                    JobUpdate = new JobUpdate
                    {
                        JobId = request.JobId,
                        State = JobState.Failed,
                        Progress = 1,
                        Error = new ProtocolError { Code = ErrorCode.Internal, Message = error.Message },
                    },
                }, sessionCancellation);
            }
        }, CancellationToken.None);
        if (!jobs.TryAdd(id, new ActiveJob(cancellation, task)))
        {
            cancellation.Cancel();
            admitted.SetCanceled();
            throw new InvalidOperationException("duplicate active job ID");
        }
        _ = task.ContinueWith(
            completedTask => jobs.TryRemove(id, out _),
            CancellationToken.None,
            TaskContinuationOptions.ExecuteSynchronously,
            TaskScheduler.Default);
        admitted.SetResult();
    }

    private void ValidateHello(HostHello hello)
    {
        if (hello.Node is null
            || hello.PluginId?.Value != Id || string.IsNullOrEmpty(hello.PluginName?.Value)
            || hello.MaximumCallDepth == 0 || hello.MaximumCausalDepth == 0
            || hello.MaximumArtifactChunkBytes == 0)
            throw new InvalidOperationException("HostHello does not describe the expected plugin instance");
    }

    private static async Task<PluginEnvelope> ReceiveAsync(
        IAsyncStreamReader<PluginEnvelope> incoming,
        Sender? sender,
        ulong lastHostMessageId,
        uint maximumCallDepth,
        uint maximumCausalDepth,
        CancellationToken cancellationToken)
    {
        var envelope = await ReadAsync(incoming, cancellationToken);
        ValidateEnvelope(envelope, sender, lastHostMessageId);
        ValidateTrace(envelope.Trace, maximumCallDepth, maximumCausalDepth);
        return envelope;
    }

    private static async Task<PluginEnvelope> ReadAsync(IAsyncStreamReader<PluginEnvelope> incoming, CancellationToken cancellationToken)
    {
        if (!await incoming.MoveNext(cancellationToken))
            throw new IOException("host closed the plugin stream");
        return incoming.Current;
    }

    private static void ValidateEnvelope(PluginEnvelope envelope, Sender? sender, ulong lastHostMessageId)
    {
        if (envelope.MessageId == 0 || envelope.MessageId <= lastHostMessageId)
            throw new InvalidOperationException("host message IDs must be nonzero and strictly increasing");
        if (sender is not null && (envelope.SessionId != sender.SessionId || envelope.PluginInstanceId != sender.InstanceId))
            throw new InvalidOperationException("host envelope belongs to another plugin instance");
        if (string.IsNullOrEmpty(envelope.Trace?.CorrelationId))
            throw new InvalidOperationException("host omitted correlation context");
    }

    private static void ValidateTrace(TraceContext trace, uint maximumCallDepth, uint maximumCausalDepth)
    {
        if (trace.CallDepth > maximumCallDepth || trace.CausalDepth > maximumCausalDepth)
            throw new InvalidOperationException("host envelope exceeds a negotiated trace depth limit");
    }

    private static Uri ValidateEndpoint(string value)
    {
        if (!Uri.TryCreate(value, UriKind.Absolute, out var endpoint)
            || endpoint.Scheme != Uri.UriSchemeHttp || !endpoint.IsDefaultPort && endpoint.Port <= 0
            || endpoint.Port <= 0 || endpoint.UserInfo.Length != 0 || endpoint.PathAndQuery != "/"
            || !(endpoint.Host.Equals("localhost", StringComparison.OrdinalIgnoreCase)
                || IPAddress.TryParse(endpoint.Host, out var address) && IPAddress.IsLoopback(address)))
            throw new ArgumentException("OLL_PLUGIN_ENDPOINT must be an http loopback URL with an explicit port", nameof(value));
        return endpoint;
    }

    private static void ValidatePluginId(string value)
    {
        var labels = value.Split('.');
        if (System.Text.Encoding.UTF8.GetByteCount(value) > 191 || labels.Length < 2
            || labels.Any(label => !Regex.IsMatch(label, "^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$")))
            throw new ArgumentException("plugin ID must be a lower-case ASCII dotted DNS name", nameof(value));
    }

    internal static bool IsCanonicalUuidV4(string value)
        => Guid.TryParseExact(value, "D", out var id)
            && value == id.ToString("D")
            && value[14] == '4'
            && value[19] is '8' or '9' or 'a' or 'b';

    private static async Task ConsumeUntilEofAsync(Stream input, CancellationToken cancellationToken)
    {
        var buffer = new byte[1024];
        while (await input.ReadAsync(buffer, cancellationToken) != 0) { }
    }

    private static async Task IgnoreCancellation(Task task)
    {
        try { await task; } catch (OperationCanceledException) { }
    }

    private sealed record RegisteredAction(
        string Description,
        Func<ActionContext, IReadOnlyList<string>, Task<ActionResult>> Handler);
    private sealed record ActiveJob(CancellationTokenSource Cancellation, Task Task);
}
