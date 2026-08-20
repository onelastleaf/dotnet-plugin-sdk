using System.Net;

namespace Onelastleaf.PluginSdk;

internal static class PluginEndpoint
{
    internal static Uri Parse(string value)
    {
        ArgumentException.ThrowIfNullOrEmpty(value);
        if (!HasExplicitPort(value)
            || !Uri.TryCreate(value, UriKind.Absolute, out var endpoint)
            || endpoint.Scheme != Uri.UriSchemeHttp
            || endpoint.Port <= 0
            || endpoint.UserInfo.Length != 0
            || value.IndexOfAny(['?', '#']) >= 0
            || endpoint.AbsolutePath != "/"
            || endpoint.Query.Length != 0
            || endpoint.Fragment.Length != 0
            || !(endpoint.Host.Equals("localhost", StringComparison.OrdinalIgnoreCase)
                || IPAddress.TryParse(endpoint.Host, out var address) && IPAddress.IsLoopback(address)))
            throw new ArgumentException(
                "OLL_PLUGIN_ENDPOINT must be an http loopback URL with an explicit port and no path, query, or fragment",
                nameof(value));
        return endpoint;
    }

    private static bool HasExplicitPort(string value)
    {
        const string SchemeSeparator = "://";
        var schemeEnd = value.IndexOf(SchemeSeparator, StringComparison.Ordinal);
        if (schemeEnd < 0)
            return false;
        var authorityStart = schemeEnd + SchemeSeparator.Length;
        var authorityEnd = value.IndexOfAny(['/', '?', '#'], authorityStart);
        if (authorityEnd < 0)
            authorityEnd = value.Length;
        var authority = value.AsSpan(authorityStart, authorityEnd - authorityStart);
        if (authority.IsEmpty || authority.IndexOf('@') >= 0)
            return false;

        var portSeparator = authority[0] == '['
            ? authority.IndexOf(']') + 1
            : authority.LastIndexOf(':');
        if (portSeparator <= 0 || portSeparator >= authority.Length - 1
            || authority[portSeparator] != ':')
            return false;
        var port = authority[(portSeparator + 1)..];
        return port.IndexOfAnyExceptInRange('0', '9') < 0;
    }
}
