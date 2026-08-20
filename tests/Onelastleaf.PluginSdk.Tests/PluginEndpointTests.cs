namespace Onelastleaf.PluginSdk.Tests;

public sealed class PluginEndpointTests
{
    [Theory]
    [InlineData("http://localhost:1234")]
    [InlineData("http://127.0.0.1:80/")]
    [InlineData("http://[::1]:65535")]
    public void ParseAcceptsExplicitLoopbackEndpoints(string value)
    {
        var endpoint = PluginEndpoint.Parse(value);

        Assert.Equal("http", endpoint.Scheme);
    }

    [Theory]
    [InlineData("http://localhost")]
    [InlineData("http://localhost/")]
    [InlineData("http://localhost:1234/path")]
    [InlineData("http://localhost:1234?query")]
    [InlineData("http://localhost:1234?")]
    [InlineData("http://localhost:1234/#fragment")]
    [InlineData("http://localhost:1234/#")]
    [InlineData("http://user@localhost:1234")]
    [InlineData("https://localhost:1234")]
    [InlineData("http://192.0.2.1:1234")]
    [InlineData("http://localhost:0")]
    public void ParseRejectsAnythingOutsideTheExactEndpointContract(string value)
    {
        Assert.Throws<ArgumentException>(() => PluginEndpoint.Parse(value));
    }
}
