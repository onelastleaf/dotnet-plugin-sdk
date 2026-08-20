namespace Onelastleaf.PluginSdk.Tests;

public sealed class ActiveJobTests
{
    [Fact]
    public async Task CompletedJobDisposesItsCancellationSource()
    {
        var cancellation = new CancellationTokenSource();
        var job = new ActiveJob(cancellation);
        job.SetTask(Task.CompletedTask);

        await job.MarkCompleted();

        Assert.Throws<ObjectDisposedException>(() => cancellation.Token);
    }

    [Fact]
    public async Task DisposalWaitsForAsynchronousCancellationCallbacks()
    {
        var callbackStarted = new TaskCompletionSource(
            TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseCallback = new TaskCompletionSource(
            TaskCreationOptions.RunContinuationsAsynchronously);
        var cancellation = new CancellationTokenSource();
        var token = cancellation.Token;
        using var registration = token.Register(() =>
        {
            callbackStarted.TrySetResult();
            releaseCallback.Task.GetAwaiter().GetResult();
        });
        var job = new ActiveJob(cancellation);
        job.SetTask(Task.CompletedTask);

        var cancelling = job.CancelAsync();
        await callbackStarted.Task.WaitAsync(TestTimeout.Value);
        var disposed = job.MarkCompleted();
        Assert.Equal(token, cancellation.Token);

        releaseCallback.TrySetResult();
        await cancelling.WaitAsync(TestTimeout.Value);
        await disposed.WaitAsync(TestTimeout.Value);

        Assert.Throws<ObjectDisposedException>(() => cancellation.Token);
    }
}
