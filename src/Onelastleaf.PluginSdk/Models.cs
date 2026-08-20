using System.Diagnostics.CodeAnalysis;
using Google.Protobuf.WellKnownTypes;
using Oll.Protocol;

namespace Onelastleaf.PluginSdk;

/// <summary>Represents the successful terminal value of an action.</summary>
public sealed class ActionResult
{
    private readonly ConfigValue? _result;
    private readonly IReadOnlyList<StoredArtifact> _artifacts;

    /// <summary>Creates a successful action result.</summary>
    /// <param name="result">The optional structured result value.</param>
    /// <param name="artifacts">Artifacts that this action already stored through its context.</param>
    public ActionResult(ConfigValue? result = null, IEnumerable<StoredArtifact>? artifacts = null)
    {
        _result = result?.Clone();
        var stored = artifacts?.ToArray() ?? [];
        if (stored.Any(static artifact => artifact is null))
            throw new ArgumentException("stored artifacts must not contain null", nameof(artifacts));
        _artifacts = Array.AsReadOnly(stored);
    }

    /// <summary>Gets a defensive copy of the structured result, if present.</summary>
    public ConfigValue? Result => _result?.Clone();

    /// <summary>Gets the artifacts that were acknowledged as stored by oll.</summary>
    public IReadOnlyList<StoredArtifact> Artifacts => _artifacts;

    /// <summary>Creates a result containing a string value.</summary>
    public static ActionResult FromString(string value)
    {
        ArgumentNullException.ThrowIfNull(value);
        return new ActionResult(new ConfigValue { StringValue = value });
    }

    internal ConfigValue? ToProtocolResult() => _result?.Clone();
}

/// <summary>
/// Identifies an artifact that oll has acknowledged as stored. Instances can
/// only be obtained from <see cref="ActionContext.StoreArtifactAsync"/>.
/// </summary>
public sealed class StoredArtifact
{
    private readonly ArtifactDescriptor _descriptor;

    internal StoredArtifact(ArtifactDescriptor descriptor) => _descriptor = descriptor.Clone();

    /// <summary>Gets a defensive copy of the stored artifact descriptor.</summary>
    public ArtifactDescriptor Descriptor => _descriptor.Clone();

    internal ArtifactDescriptor ToProtocol() => _descriptor.Clone();
}

/// <summary>Provides the job identity, cancellation, and host capabilities for one action call.</summary>
public sealed class ActionContext
{
    private readonly Timestamp? _deadline;
    private readonly TraceContext _trace;
    private readonly ulong _parentCallId;
    private readonly Host _host;

    internal ActionContext(
        string jobId,
        Timestamp? deadline,
        TraceContext trace,
        ulong parentCallId,
        Host host,
        CancellationToken cancellationToken)
    {
        JobId = jobId;
        _deadline = deadline?.Clone();
        _trace = trace.Clone();
        _parentCallId = parentCallId;
        _host = host;
        CancellationToken = cancellationToken;
    }

    /// <summary>Gets the host-assigned canonical job ID.</summary>
    public string JobId { get; }

    /// <summary>Gets a defensive copy of the optional action deadline.</summary>
    public Timestamp? Deadline => _deadline?.Clone();

    /// <summary>Gets a defensive copy of the action's trace context.</summary>
    public TraceContext Trace => _trace.Clone();

    /// <summary>Gets the token that is cancelled for this job or the whole plugin session.</summary>
    public CancellationToken CancellationToken { get; }

    /// <summary>Invokes a permitted oll host capability.</summary>
    public Task<HostCallResponse> HostCallAsync(HostCallRequest request)
    {
        ArgumentNullException.ThrowIfNull(request);
        return _host.CallAsync(NestedTrace(), request, CancellationToken);
    }

    /// <summary>Reads this plugin's current oll-managed configuration.</summary>
    public Task<GetConfigResponse> GetConfigAsync(ConfigPath? path = null)
        => _host.GetConfigAsync(NestedTrace(), path, CancellationToken);

    /// <summary>Invokes a function reference returned by plugin configuration.</summary>
    public Task<InvokeConfigFunctionResponse> InvokeConfigFunctionAsync(
        ConfigFunctionRef function,
        IEnumerable<ConfigValue> arguments)
    {
        ArgumentNullException.ThrowIfNull(function);
        ArgumentNullException.ThrowIfNull(arguments);
        return _host.InvokeConfigFunctionAsync(
            NestedTrace(),
            function,
            arguments,
            CancellationToken);
    }

    /// <summary>Writes a structured record to this plugin's oll-managed log.</summary>
    public Task LogAsync(
        LogLevel level,
        string target,
        string message,
        IReadOnlyDictionary<string, ConfigValue>? fields = null)
        => _host.LogAsync(
            _trace.Clone(),
            level,
            target,
            message,
            fields,
            CancellationToken);

    /// <summary>Validates and streams an artifact to oll using bounded memory.</summary>
    public Task<StoredArtifact> StoreArtifactAsync(
        ArtifactDescriptor artifact,
        Stream content,
        int? chunkSize = null)
    {
        ArgumentNullException.ThrowIfNull(artifact);
        ArgumentNullException.ThrowIfNull(content);
        return _host.StoreArtifactAsync(
            NestedTrace(),
            JobId,
            artifact,
            content,
            chunkSize,
            CancellationToken);
    }

    private TraceContext NestedTrace()
    {
        var nested = _trace.Clone();
        nested.ParentCallId = _parentCallId;
        if (nested.CallDepth == uint.MaxValue)
            throw new InvalidOperationException("host-call depth overflowed");
        nested.CallDepth++;
        if (nested.CallDepth > _host.MaximumCallDepth)
            throw new InvalidOperationException("host call exceeds the negotiated call-depth limit");
        return nested;
    }
}

/// <summary>Reports a structured, expected action failure to oll.</summary>
[SuppressMessage(
    "Design",
    "CA1032:Implement standard exception constructors",
    Justification = "A ProtocolError is required to preserve the structured failure contract.")]
public sealed class ActionFailureException : Exception
{
    private readonly ProtocolError _error;

    /// <summary>Creates a structured action failure.</summary>
    public ActionFailureException(ProtocolError error)
        : base(CreateMessage(error)) => _error = error.Clone();

    /// <summary>Gets a defensive copy of the protocol error sent to oll.</summary>
    public ProtocolError Error => _error.Clone();

    private static string CreateMessage(ProtocolError error)
    {
        ArgumentNullException.ThrowIfNull(error);
        return $"action failed ({error.Code}): {error.Message}";
    }
}

/// <summary>Indicates that oll rejected a plugin-to-host request.</summary>
[SuppressMessage(
    "Design",
    "CA1032:Implement standard exception constructors",
    Justification = "A ProtocolError is required to preserve the structured host response.")]
public sealed class HostProtocolException : Exception
{
    private readonly ProtocolError _error;

    internal HostProtocolException(ProtocolError error)
        : base($"host rejected request ({error.Code}): {error.Message}") => _error = error.Clone();

    /// <summary>Gets a defensive copy of the host's structured error.</summary>
    public ProtocolError Error => _error.Clone();
}
