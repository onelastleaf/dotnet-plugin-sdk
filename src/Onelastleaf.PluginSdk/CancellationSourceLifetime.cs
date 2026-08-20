namespace Onelastleaf.PluginSdk;

internal static class CancellationSourceLifetime
{
    // Cancellation callbacks are user-extensible and may run asynchronously.
    // Dispose only after they finish; callers never need to block session exit.
    internal static void CancelAndDispose(CancellationTokenSource cancellation)
    {
        ArgumentNullException.ThrowIfNull(cancellation);
        Task task;
        try
        {
            task = cancellation.CancelAsync();
        }
        catch
        {
            cancellation.Dispose();
            throw;
        }
        if (task.IsCompleted)
        {
            _ = task.Exception;
            cancellation.Dispose();
            return;
        }
        _ = task.ContinueWith(
            completed =>
            {
                _ = completed.Exception;
                cancellation.Dispose();
            },
            CancellationToken.None,
            TaskContinuationOptions.ExecuteSynchronously,
            TaskScheduler.Default);
    }
}
