namespace Onelastleaf.PluginSdk;

// Owns the linked cancellation source for exactly one admitted job. Disposal
// waits for both the handler and asynchronous cancellation callbacks.
internal sealed class ActiveJob(CancellationTokenSource cancellation)
{
    private readonly object _gate = new();
    private readonly CancellationTokenSource _cancellation = cancellation;
    private readonly TaskCompletionSource _disposedCompletion =
        new(TaskCreationOptions.RunContinuationsAsynchronously);
    private Task? _task;
    private Task _cancellationTask = Task.CompletedTask;
    private bool _completed;
    private bool _disposed;
    private bool _hostCancellationStarted;

    internal CancellationToken Token => _cancellation.Token;
    internal bool IsCancellationRequested => _cancellation.IsCancellationRequested;
    internal Task Task => _task
        ?? throw new InvalidOperationException("job task has not been set");

    internal void SetTask(Task task)
    {
        ArgumentNullException.ThrowIfNull(task);
        lock (_gate)
        {
            if (_task is not null)
                throw new InvalidOperationException("job task is already set");
            _task = task;
        }
    }

    internal Task CancelAsync()
    {
        lock (_gate)
        {
            return StartCancellationUnderLock();
        }
    }

    internal bool TryBeginHostCancellation(out Task cancellation)
    {
        lock (_gate)
        {
            if (_hostCancellationStarted)
            {
                cancellation = _cancellationTask;
                return false;
            }
            _hostCancellationStarted = true;
            cancellation = StartCancellationUnderLock();
            return true;
        }
    }

    internal Task MarkCompleted()
    {
        lock (_gate)
        {
            _completed = true;
            if (_cancellationTask.IsCompleted)
            {
                DisposeUnderLock();
                return _disposedCompletion.Task;
            }
            _ = _cancellationTask.ContinueWith(
                _ => DisposeIfCompleted(),
                CancellationToken.None,
                TaskContinuationOptions.ExecuteSynchronously,
                TaskScheduler.Default);
            return _disposedCompletion.Task;
        }
    }

    private void DisposeIfCompleted()
    {
        lock (_gate)
        {
            if (_completed)
                DisposeUnderLock();
        }
    }

    private Task StartCancellationUnderLock()
    {
        if (_disposed)
            return Task.CompletedTask;
        if (!_cancellation.IsCancellationRequested)
            _cancellationTask = _cancellation.CancelAsync();
        return _cancellationTask;
    }

    private void DisposeUnderLock()
    {
        if (_disposed)
            return;
        _ = _cancellationTask.Exception;
        _cancellation.Dispose();
        _disposed = true;
        _disposedCompletion.TrySetResult();
    }
}
