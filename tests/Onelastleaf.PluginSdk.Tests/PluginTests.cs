using Xunit;
namespace Onelastleaf.PluginSdk.Tests;

public sealed class PluginTests
{
    [Fact]
    public void ValidatesIdentityAndResults()
    {
        Assert.Throws<ArgumentException>(() => Plugin.Create("invalid", "0.1.0"));
        Assert.Equal("value", ActionResult.String("value").Result?.StringValue);
        Assert.True(Plugin.IsCanonicalUuidV4("0f337c0c-51d6-44a9-a691-a31fce775ab1"));
        Assert.False(Plugin.IsCanonicalUuidV4("0f337c0c-51d6-14a9-a691-a31fce775ab1"));
    }
}
