using System.Globalization;
using Oll.Protocol;

namespace Onelastleaf.PluginSdk.Tests;

public sealed class SenderTests
{
    [Fact]
    public async Task DisposeLetsAnAdmittedWriteReleaseItsResources()
    {
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var writer = new TestClientStreamWriter(async _ =>
        {
            entered.TrySetResult();
            await release.Task;
        });
        var sender = new Sender(writer);
        var send = sender.SendAsync(
            null,
            TestProtocol.Trace(),
            new PluginEnvelope { Heartbeat = new Heartbeat() });
        await entered.Task.WaitAsync(TestTimeout.Value);

        sender.Dispose();
        release.TrySetResult();

        Assert.Equal(1ul, await send);
        await Assert.ThrowsAsync<ObjectDisposedException>(() => sender.SendAsync(
            null,
            TestProtocol.Trace(),
            new PluginEnvelope { Heartbeat = new Heartbeat() }));
    }

    [Fact]
    public async Task SendQueueRejectsUnboundedWaiters()
    {
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using var cancellation = new CancellationTokenSource();
        var writer = new TestClientStreamWriter(async token =>
        {
            entered.TrySetResult();
            await release.Task.WaitAsync(token);
        });
        using var sender = new Sender(writer);
        var sends = Enumerable.Range(0, Sender.MaximumQueuedSends)
            .Select(_ => sender.SendAsync(
                null,
                TestProtocol.Trace(),
                new PluginEnvelope { Heartbeat = new Heartbeat() },
                cancellationToken: cancellation.Token))
            .ToArray();
        await entered.Task.WaitAsync(TestTimeout.Value);

        var error = await Assert.ThrowsAsync<InvalidOperationException>(() => sender.SendAsync(
            null,
            TestProtocol.Trace(),
            new PluginEnvelope { Heartbeat = new Heartbeat() },
            cancellationToken: cancellation.Token));

        Assert.Contains(
            Sender.MaximumQueuedSends.ToString(CultureInfo.InvariantCulture),
            error.Message,
            StringComparison.Ordinal);
        await cancellation.CancelAsync();
        release.TrySetResult();
        try
        {
            await Task.WhenAll(sends);
        }
        catch (OperationCanceledException)
        {
        }
    }
}
