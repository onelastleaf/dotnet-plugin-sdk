using Oll.Protocol;

namespace Onelastleaf.PluginSdk;

public sealed record ActionResult(ConfigValue? Result = null, IReadOnlyList<ArtifactDescriptor>? Artifacts = null)
{
    public static ActionResult String(string value) => new(new ConfigValue { StringValue = value });
}

public sealed class ActionContext
{
    internal ActionContext(
        string jobId,
        Google.Protobuf.WellKnownTypes.Timestamp? deadline,
        TraceContext trace,
        ulong parentCallId,
        Host host,
        CancellationToken cancellationToken)
    {
        JobId = jobId;
        Deadline = deadline;
        Trace = trace.Clone();
        ParentCallId = parentCallId;
        Host = host;
        CancellationToken = cancellationToken;
    }

    public string JobId { get; }
    public Google.Protobuf.WellKnownTypes.Timestamp? Deadline { get; }
    public TraceContext Trace { get; }
    private ulong ParentCallId { get; }
    public Host Host { get; }
    public CancellationToken CancellationToken { get; }

    public Task<HostCallResponse> HostCallAsync(HostCallRequest request)
        => Host.CallAsync(NestedTrace(), request, CancellationToken);

    public Task<GetConfigResponse> GetConfigAsync(ConfigPath? path = null)
        => Host.GetConfigAsync(NestedTrace(), path, CancellationToken);

    public Task<InvokeConfigFunctionResponse> InvokeConfigFunctionAsync(
        ConfigFunctionRef function,
        IEnumerable<ConfigValue> arguments)
        => Host.InvokeConfigFunctionAsync(
            NestedTrace(),
            function,
            arguments,
            CancellationToken);

    private TraceContext NestedTrace()
    {
        if (Trace.CallDepth == uint.MaxValue)
            throw new InvalidOperationException("host-call depth overflowed");
        var nested = Trace.Clone();
        nested.ParentCallId = ParentCallId;
        nested.CallDepth++;
        if (nested.CallDepth > Host.MaximumCallDepth)
            throw new InvalidOperationException("host call exceeds the negotiated call-depth limit");
        return nested;
    }

}

public sealed class HostProtocolException : Exception
{
    internal HostProtocolException(ProtocolError error)
        : base($"host rejected request ({error.Code}): {error.Message}") => Error = error;

    public ProtocolError Error { get; }
}
