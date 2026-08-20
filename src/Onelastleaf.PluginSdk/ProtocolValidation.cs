using Oll.Protocol;

namespace Onelastleaf.PluginSdk;

internal static class ProtocolValidation
{
    internal static bool IsCanonicalUuidV4(string value)
        => Guid.TryParseExact(value, "D", out var id)
            && value == id.ToString("D")
            && value[14] == '4'
            && value[19] is '8' or '9' or 'a' or 'b';

    internal static void ValidateTrace(
        TraceContext trace,
        uint maximumCallDepth,
        uint maximumCausalDepth)
    {
        ArgumentNullException.ThrowIfNull(trace);
        if (trace.CallDepth > maximumCallDepth
            || trace.CausalDepth > maximumCausalDepth)
            throw new InvalidDataException(
                "host envelope exceeds a negotiated trace-depth limit");
    }
}
