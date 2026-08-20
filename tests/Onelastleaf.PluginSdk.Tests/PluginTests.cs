namespace Onelastleaf.PluginSdk.Tests;

public sealed class PluginTests
{
    [Fact]
    public void CreateRejectsSingleLabelId()
    {
        var error = Assert.Throws<ArgumentException>(() => Plugin.Create("invalid", "1.0.0"));

        Assert.Equal("value", error.ParamName);
    }

    [Fact]
    public void ActionsCannotChangeAfterRunStarts()
    {
        var plugin = Plugin.Create("dev.example.plugin", "1.0.0");
        _ = plugin.FreezeActions();

        Assert.Throws<InvalidOperationException>(() => plugin.Action(
            "late",
            "too late",
            (_, _) => Task.FromResult(new ActionResult())));
    }

    [Fact]
    public async Task ParentLivenessFailureWinsOverCancellationNoise()
    {
        var expected = new IOException("stdin failed");
        using var input = new FaultingReadStream(expected);
        var plugin = Plugin.Create("dev.example.plugin", "1.0.0");

        var actual = await Assert.ThrowsAsync<IOException>(() =>
            plugin.RunAtAsync("http://127.0.0.1:1", input, CancellationToken.None));

        Assert.Same(expected, actual);
    }

    [Fact]
    public void NegotiatedTraceDepthAppliesToHostHelloItself()
    {
        var trace = TestProtocol.Trace();
        trace.CallDepth = 2;

        Assert.Throws<InvalidDataException>(() =>
            ProtocolValidation.ValidateTrace(trace, maximumCallDepth: 1, maximumCausalDepth: 1));
    }

    [Fact]
    public async Task CompletedSessionDoesNotWaitForUncooperativeParentRead()
    {
        var parentEof = new TaskCompletionSource(
            TaskCreationOptions.RunContinuationsAsynchronously);
        using var sessionCancellation = new CancellationTokenSource();
        using var monitorCancellation = new CancellationTokenSource();

        await Plugin.CoordinateLifetimeAsync(
            Task.CompletedTask,
            parentEof.Task,
            sessionCancellation,
            monitorCancellation).WaitAsync(TestTimeout.Value);

        Assert.True(monitorCancellation.IsCancellationRequested);
        parentEof.TrySetResult();
    }
}
