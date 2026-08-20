using System.Diagnostics.CodeAnalysis;
using System.Text;
using System.Text.RegularExpressions;
using Grpc.Core;
using Grpc.Net.Client;
using Oll.Protocol;

namespace Onelastleaf.PluginSdk;

/// <summary>Builds and runs one trusted onelastleaf process plugin.</summary>
public sealed partial class Plugin
{
    internal const string EndpointEnvironmentVariable = "OLL_PLUGIN_ENDPOINT";
    private const int MaximumEnvelopeBytes = 64 * 1024 * 1024;
    private const int ParentLivenessBufferBytes = 1024;

    private readonly object _gate = new();
    private readonly Dictionary<string, RegisteredAction> _actions = [];
    private bool _hasRun;

    private Plugin(string id, string version) => (Id, Version) = (id, version);

    /// <summary>Gets this plugin's immutable publisher ID.</summary>
    public string Id { get; }

    /// <summary>Gets the plugin build version reported during the handshake.</summary>
    public string Version { get; }

    /// <summary>Creates a plugin definition.</summary>
    public static Plugin Create(string id, string version)
    {
        ValidatePluginId(id);
        ArgumentException.ThrowIfNullOrEmpty(version);
        return new Plugin(id, version);
    }

    /// <summary>Registers an action before the plugin starts.</summary>
    public Plugin Action(
        string name,
        string description,
        Func<ActionContext, IReadOnlyList<string>, Task<ActionResult>> handler)
    {
        ArgumentException.ThrowIfNullOrEmpty(name);
        ArgumentNullException.ThrowIfNull(description);
        ArgumentNullException.ThrowIfNull(handler);
        lock (_gate)
        {
            if (_hasRun)
                throw new InvalidOperationException("actions cannot be changed after the plugin starts");
            if (!_actions.TryAdd(name, new RegisteredAction(description, handler)))
                throw new ArgumentException($"duplicate action {name}", nameof(name));
        }
        return this;
    }

    /// <summary>
    /// Connects to the oll-owned endpoint and runs until shutdown, parent EOF,
    /// cancellation, or a protocol failure.
    /// </summary>
    public async Task RunAsync(CancellationToken cancellationToken = default)
    {
        var endpoint = Environment.GetEnvironmentVariable(EndpointEnvironmentVariable)
            ?? throw new InvalidOperationException($"{EndpointEnvironmentVariable} is required");
        using var parentLiveness = Console.OpenStandardInput();
        await RunAtAsync(endpoint, parentLiveness, cancellationToken).ConfigureAwait(false);
    }

    internal async Task RunAtAsync(
        string endpoint,
        Stream parentLiveness,
        CancellationToken cancellationToken)
    {
        var uri = PluginEndpoint.Parse(endpoint);
        ArgumentNullException.ThrowIfNull(parentLiveness);
        if (!parentLiveness.CanRead)
            throw new ArgumentException("parent-liveness stream must be readable", nameof(parentLiveness));
        var actions = FreezeActions();

        var sessionCancellation = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        var monitorCancellation = new CancellationTokenSource();
        var parentEof = ConsumeUntilEofAsync(parentLiveness, monitorCancellation.Token);
        var session = RunConnectionAsync(uri, actions, sessionCancellation.Token);
        await CoordinateLifetimeAsync(
            session,
            parentEof,
            sessionCancellation,
            monitorCancellation).ConfigureAwait(false);
    }

    [SuppressMessage(
        "Design",
        "CA1031:Do not catch general exception types",
        Justification = "Any parent-liveness read failure must cancel the session and then be rethrown unchanged.")]
    internal static async Task CoordinateLifetimeAsync(
        Task session,
        Task parentEof,
        CancellationTokenSource sessionCancellation,
        CancellationTokenSource monitorCancellation)
    {
        var sessionDisposalDeferred = false;
        var monitorDisposalDeferred = false;
        try
        {
            var completed = await Task.WhenAny(session, parentEof).ConfigureAwait(false);
            if (completed == parentEof)
            {
                Exception? parentError = null;
                try
                {
                    await parentEof.ConfigureAwait(false);
                }
                catch (Exception error)
                {
                    parentError = error;
                }

                CancellationSourceLifetime.CancelAndDispose(sessionCancellation);
                sessionDisposalDeferred = true;
                try
                {
                    await session.ConfigureAwait(false);
                }
                catch (OperationCanceledException) when (parentError is null)
                {
                }
                catch (RpcException error) when (
                    parentError is null && error.StatusCode == StatusCode.Cancelled)
                {
                }
                catch (Exception) when (parentError is not null)
                {
                    // The parent-liveness failure is the initiating error and is
                    // rethrown below after the cancelled session has unwound.
                }

                if (parentError is not null)
                    System.Runtime.ExceptionServices.ExceptionDispatchInfo.Capture(parentError).Throw();
                return;
            }

            // A Stream is allowed to ignore cancellation. Session shutdown must not
            // wait forever for such a stdin read; RunAsync disposes its owned stdin
            // stream immediately after this coordinator returns.
            CancellationSourceLifetime.CancelAndDispose(monitorCancellation);
            monitorDisposalDeferred = true;
            Observe(parentEof);
            await session.ConfigureAwait(false);
        }
        finally
        {
            if (!sessionDisposalDeferred)
                sessionCancellation.Dispose();
            if (!monitorDisposalDeferred)
                monitorCancellation.Dispose();
        }
    }

    private async Task RunConnectionAsync(
        Uri endpoint,
        IReadOnlyDictionary<string, RegisteredAction> actions,
        CancellationToken cancellationToken)
    {
        using var channel = GrpcChannel.ForAddress(endpoint, new GrpcChannelOptions
        {
            MaxReceiveMessageSize = MaximumEnvelopeBytes,
            MaxSendMessageSize = MaximumEnvelopeBytes,
        });
        var client = new PluginRuntime.PluginRuntimeClient(channel);
        using var stream = client.Connect(cancellationToken: cancellationToken);
        using var sender = new Sender(stream.RequestStream);

        var first = await ReceiveAsync(
            stream.ResponseStream,
            sender: null,
            lastHostMessageId: 0,
            maximumCallDepth: uint.MaxValue,
            maximumCausalDepth: uint.MaxValue,
            cancellationToken).ConfigureAwait(false);
        if (first.HasReplyTo || first.PayloadCase != PluginEnvelope.PayloadOneofCase.HostHello)
            throw new InvalidDataException("HostHello must be the first host message");
        if (first.SessionId.Length == 0 || first.PluginInstanceId.Length == 0)
            throw new InvalidDataException("HostHello omitted its session or instance identity");
        ValidateHello(first.HostHello);
        ProtocolValidation.ValidateTrace(
            first.Trace,
            first.HostHello.MaximumCallDepth,
            first.HostHello.MaximumCausalDepth);
        sender.SetIdentity(first.SessionId, first.PluginInstanceId);

        var hello = new PluginHello
        {
            PluginId = new PluginId { Value = Id },
            PluginName = first.HostHello.PluginName.Clone(),
            PluginVersion = Version,
        };
        hello.Actions.AddRange(actions.Select(static action => new ActionDescriptor
        {
            Name = action.Key,
            Description = action.Value.Description,
        }));
        await sender.SendAsync(
            null,
            first.Trace,
            new PluginEnvelope { PluginHello = hello },
            cancellationToken: cancellationToken).ConfigureAwait(false);

        var ready = await ReceiveAsync(
            stream.ResponseStream,
            sender,
            first.MessageId,
            first.HostHello.MaximumCallDepth,
            first.HostHello.MaximumCausalDepth,
            cancellationToken).ConfigureAwait(false);
        if (ready.HasReplyTo
            || ready.PayloadCase != PluginEnvelope.PayloadOneofCase.Ready
            || !ready.Trace.Equals(first.Trace))
            throw new InvalidDataException("host SessionReady must follow PluginHello with the same trace");
        await sender.SendAsync(
            null,
            first.Trace,
            new PluginEnvelope { Ready = new SessionReady() },
            cancellationToken: cancellationToken).ConfigureAwait(false);

        var host = new Host(
            sender,
            first.HostHello.MaximumArtifactChunkBytes,
            first.HostHello.MaximumCallDepth,
            first.HostHello.MaximumCausalDepth);
        var session = new PluginSession(stream.ResponseStream, sender, host, actions);
        await session.RunAsync(ready.MessageId, cancellationToken).ConfigureAwait(false);
        await stream.RequestStream.CompleteAsync().ConfigureAwait(false);
    }

    internal Dictionary<string, RegisteredAction> FreezeActions()
    {
        lock (_gate)
        {
            if (_hasRun)
                throw new InvalidOperationException("a Plugin instance can only be run once");
            _hasRun = true;
            return new Dictionary<string, RegisteredAction>(_actions, StringComparer.Ordinal);
        }
    }

    private void ValidateHello(HostHello hello)
    {
        if (hello.Node is null
            || hello.PluginId?.Value != Id
            || string.IsNullOrEmpty(hello.PluginName?.Value)
            || hello.MaximumCallDepth == 0
            || hello.MaximumCausalDepth == 0
            || hello.MaximumArtifactChunkBytes == 0)
            throw new InvalidDataException(
                "HostHello does not describe the expected plugin instance");
    }

    private static async Task<PluginEnvelope> ReceiveAsync(
        IAsyncStreamReader<PluginEnvelope> incoming,
        Sender? sender,
        ulong lastHostMessageId,
        uint maximumCallDepth,
        uint maximumCausalDepth,
        CancellationToken cancellationToken)
    {
        if (!await incoming.MoveNext(cancellationToken).ConfigureAwait(false))
            throw new IOException("host closed the plugin stream");
        var envelope = incoming.Current;
        if (envelope.MessageId == 0 || envelope.MessageId <= lastHostMessageId)
            throw new InvalidDataException(
                "host message IDs must be nonzero and strictly increasing");
        if (sender is not null
            && (envelope.SessionId != sender.SessionId
                || envelope.PluginInstanceId != sender.InstanceId))
            throw new InvalidDataException("host envelope belongs to another plugin instance");
        if (string.IsNullOrEmpty(envelope.Trace?.CorrelationId))
            throw new InvalidDataException("host omitted correlation context");
        ProtocolValidation.ValidateTrace(
            envelope.Trace,
            maximumCallDepth,
            maximumCausalDepth);
        return envelope;
    }

    private static void ValidatePluginId(string value)
    {
        ArgumentException.ThrowIfNullOrEmpty(value);
        var labels = value.Split('.');
        if (Encoding.UTF8.GetByteCount(value) > 191
            || labels.Length < 2
            || labels.Any(static label => !PluginIdLabel().IsMatch(label)))
            throw new ArgumentException(
                "plugin ID must be a lower-case ASCII dotted DNS name",
                nameof(value));
    }

    private static async Task ConsumeUntilEofAsync(
        Stream input,
        CancellationToken cancellationToken)
    {
        var buffer = new byte[ParentLivenessBufferBytes];
        while (await input.ReadAsync(buffer, cancellationToken).ConfigureAwait(false) != 0)
        {
        }
    }

    private static void Observe(Task task)
    {
        if (task.IsCompleted)
        {
            _ = task.Exception;
            return;
        }
        _ = task.ContinueWith(
            completed => _ = completed.Exception,
            CancellationToken.None,
            TaskContinuationOptions.ExecuteSynchronously | TaskContinuationOptions.OnlyOnFaulted,
            TaskScheduler.Default);
    }

    [GeneratedRegex("^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$")]
    private static partial Regex PluginIdLabel();
}
