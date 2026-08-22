using System.Xml.Linq;

namespace Onelastleaf.PluginSdk.Tests;

public sealed class PackagingTests
{
    [Fact]
    public void PublicLicenseDeclarationsAgreeOnGpl3OrLater()
    {
        var fixtureDirectory = Path.Combine(AppContext.BaseDirectory, "PackageUnderTest");
        var project = XDocument.Load(
            Path.Combine(fixtureDirectory, "Onelastleaf.PluginSdk.csproj"));
        var expression = Assert.Single(
            project.Descendants("PackageLicenseExpression")).Value;

        Assert.Equal("GPL-3.0-or-later", expression);

        var readme = File.ReadAllText(Path.Combine(fixtureDirectory, "README.md"));
        Assert.Contains($"[{expression}](LICENSE)", readme, StringComparison.Ordinal);

        var license = File.ReadAllText(Path.Combine(fixtureDirectory, "LICENSE"));
        Assert.Contains("either version 3 of the License, or", license, StringComparison.Ordinal);
        Assert.Contains("(at your option) any later version.", license, StringComparison.Ordinal);
    }
}
