using Xunit;
using Google.Protobuf;
using Oll.Protocol;

namespace Onelastleaf.PluginSdk.Tests;

public sealed class PluginTests
{
    [Fact]
    public void ValidatesIdentityAndPublishesExactFingerprint()
    {
        Assert.Throws<ArgumentException>(() => Plugin.Create("invalid", "0.1.0"));
        Assert.Equal(
            "21c145638fbe6a1f2d9a2cb2114403d4bee4da3c0adbac09e805a98a77d0d4da",
            Plugin.ProtocolSchemaSha256);
        Assert.Equal("value", ActionResult.String("value").Result?.StringValue);
        Assert.True(Plugin.IsCanonicalUuidV4("0f337c0c-51d6-44a9-a691-a31fce775ab1"));
        Assert.False(Plugin.IsCanonicalUuidV4("0f337c0c-51d6-14a9-a691-a31fce775ab1"));
    }
}
