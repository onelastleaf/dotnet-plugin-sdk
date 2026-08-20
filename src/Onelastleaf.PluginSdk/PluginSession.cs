using System.Collections.Concurrent;
using System.Diagnostics.CodeAnalysis;
using Grpc.Core;
using Oll.Protocol;

namespace Onelastleaf.PluginSdk;

internal sealed class PluginSession
{
    private static readonly TimeSpan ShutdownAcknowledgementBudget = TimeSpan.FromMilliseconds(100);

    private readonly IAsyncStreamReader<PluginEnvelope> _incoming;
    private readonly Sender _sender;
    private readonly Host _host;
    private readonly IReadOnlyDictionary<string, RegisteredAction> _actions;
    private readonly TimeProvider _timeProvider;
    private readonly ConcurrentDictionary<string, ActiveJob> _jobs = [];
    private readonly ConcurrentDictionary<int, Task> _controlTasks = [];
    private readonly TaskCompletionSource<Exception> _fatalError =
        new(TaskCreationOptions.RunContinuationsAsynchronously);
    private CancellationToken _lifetimeToken;
    private ulong _lastHostMessageId;
    private int _nextControlTaskId;
    private int _stopping;
    private int _started;

    internal PluginSession(
        IAsyncStreamReader<PluginEnvelope> incoming,
        Sender sender,
        Host host,
        IReadOnlyDictionary<string, RegisteredAction> actions,
        TimeProvider? timeProvider = null)
    {
        _incoming = incoming ?? throw new ArgumentNullException(nameof(incoming));
        _sender = sender ?? throw new ArgumentNullException(nameof(sender));
        _host = host ?? throw new ArgumentNullException(nameof(host));
        _actions = actions ?? throw new ArgumentNullException(nameof(actions));
        _timeProvider = timeProvider ?? TimeProvider.System;
    }

    internal async Task RunAsync(
        ulong lastHostMessageId,
        CancellationToken cancellationToken = default)
    {
        if (Interlocked.Exchange(ref _started, 1) != 0)
            throw new InvalidOperationException("plugin session can only be run once");

        _lastHostMessageId = lastHostMessageId;
        var lifetime = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        _lifetimeToken = lifetime.Token;
        Exception? closeError = null;
        try
        {
            while (true)
            {
                var envelope = await ReadNextAsync().ConfigureAwait(false);
                ValidateEnvelope(envelope);
                ValidateTrace(envelope.Trace);
                _lastHostMessageId = envelope.MessageId;
                if (envelope.HasReplyTo)
                {
                    _host.Route(envelope);
                    continue;
                }

                switch (envelope.PayloadCase)
                {
                    case PluginEnvelope.PayloadOneofCase.StartJob:
                        await StartJobAsync(envelope).ConfigureAwait(false);
                        break;
                    case PluginEnvelope.PayloadOneofCase.CancelJob:
                        CancelJob(envelope);
                        break;
                    case PluginEnvelope.PayloadOneofCase.Heartbeat:
                        await _sender.SendAsync(
                            envelope.MessageId,
                            envelope.Trace,
                            new PluginEnvelope { Heartbeat = envelope.Heartbeat.Clone() },
                            cancellationToken: _lifetimeToken).ConfigureAwait(false);
                        break;
                    case PluginEnvelope.PayloadOneofCase.Shutdown:
                        await ShutDownAsync(envelope).ConfigureAwait(false);
                        return;
                    case PluginEnvelope.PayloadOneofCase.ProtocolError:
                        throw new HostProtocolException(envelope.ProtocolError);
                    default:
                        throw new InvalidDataException(
                            $"unexpected host-initiated {envelope.PayloadCase} message");
                }
            }
        }
        catch (Exception error)
        {
            closeError = error;
            throw;
        }
        finally
        {
            Interlocked.Exchange(ref _stopping, 1);
            _host.Close(closeError);
            foreach (var job in _jobs.Values)
                Observe(job.CancelAsync());
            CancellationSourceLifetime.CancelAndDispose(lifetime);
        }
    }

    private async Task<PluginEnvelope> ReadNextAsync()
    {
        var read = _incoming.MoveNext(_lifetimeToken);
        var completed = await Task.WhenAny(read, _fatalError.Task).ConfigureAwait(false);
        if (completed == _fatalError.Task)
        {
            Observe(read);
            throw await _fatalError.Task.ConfigureAwait(false);
        }
        if (!await read.ConfigureAwait(false))
            throw new IOException("host closed the plugin stream");
        return _incoming.Current;
    }

    [SuppressMessage(
        "Reliability",
        "CA2000:Dispose objects before losing scope",
        Justification = "ActiveJob owns and disposes the linked cancellation source after the job and cancellation callbacks finish.")]
    private async Task StartJobAsync(PluginEnvelope envelope)
    {
        if (Volatile.Read(ref _stopping) != 0)
            throw new InvalidDataException("host started a job while the plugin was stopping");
        var request = envelope.StartJob;
        var jobId = request.JobId?.Value ?? "";
        if (!ProtocolValidation.IsCanonicalUuidV4(jobId)
            || request.InvocationCase != StartJobRequest.InvocationOneofCase.Action)
            throw new InvalidDataException("host sent an invalid StartJobRequest");
        if (!_actions.TryGetValue(request.Action.Action, out var action))
            throw new InvalidDataException($"host requested unknown action {request.Action.Action}");
        var protocolJobId = request.JobId!;

        var cancellation = CancellationTokenSource.CreateLinkedTokenSource(_lifetimeToken);
        var job = new ActiveJob(cancellation);
        var admitted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var arguments = Array.AsReadOnly(request.Action.Arguments.ToArray());
        var task = RunJobAsync(
            job,
            admitted.Task,
            jobId,
            protocolJobId.Clone(),
            request.Deadline?.Clone(),
            envelope.Trace.Clone(),
            envelope.MessageId,
            arguments,
            action.Handler);
        job.SetTask(task);
        if (!_jobs.TryAdd(jobId, job))
        {
            Observe(job.CancelAsync());
            admitted.TrySetCanceled(CancellationToken.None);
            Observe(task);
            throw new InvalidDataException("host reused an active job ID");
        }
        _ = task.ContinueWith(
            completed => JobCompleted(jobId, job, completed),
            CancellationToken.None,
            TaskContinuationOptions.ExecuteSynchronously,
            TaskScheduler.Default);

        try
        {
            await _sender.SendAsync(
                envelope.MessageId,
                envelope.Trace,
                new PluginEnvelope
                {
                    JobAccepted = new JobAccepted { JobId = protocolJobId.Clone() },
                },
                cancellationToken: _lifetimeToken).ConfigureAwait(false);
            admitted.TrySetResult();
        }
        catch
        {
            Observe(job.CancelAsync());
            admitted.TrySetCanceled(CancellationToken.None);
            throw;
        }
    }

    [SuppressMessage(
        "Design",
        "CA1031:Do not catch general exception types",
        Justification = "The SDK boundary converts every unhandled plugin exception into a terminal INTERNAL job result.")]
    private async Task RunJobAsync(
        ActiveJob job,
        Task admission,
        string jobId,
        PluginJobId protocolJobId,
        Google.Protobuf.WellKnownTypes.Timestamp? deadline,
        TraceContext trace,
        ulong parentCallId,
        IReadOnlyList<string> arguments,
        Func<ActionContext, IReadOnlyList<string>, Task<ActionResult>> handler)
    {
        JobUpdate? terminal = null;
        try
        {
            await admission.ConfigureAwait(false);
            var context = new ActionContext(
                jobId,
                deadline,
                trace,
                parentCallId,
                _host,
                job.Token);
            var result = await handler(context, arguments).ConfigureAwait(false)
                ?? throw new InvalidOperationException("action returned a null result");
            job.Token.ThrowIfCancellationRequested();
            terminal = new JobUpdate
            {
                JobId = protocolJobId,
                State = JobState.Succeeded,
                Progress = 1,
                Result = result.ToProtocolResult(),
            };
            terminal.Artifacts.AddRange(result.Artifacts.Select(static artifact => artifact.ToProtocol()));
        }
        catch (OperationCanceledException) when (job.IsCancellationRequested)
        {
            return;
        }
        catch (ActionFailureException error)
        {
            terminal = FailedUpdate(protocolJobId, error.Error);
        }
        catch (HostProtocolException error)
        {
            terminal = FailedUpdate(protocolJobId, error.Error);
        }
        catch (Exception error)
        {
            terminal = FailedUpdate(
                protocolJobId,
                new ProtocolError
                {
                    Code = ErrorCode.Internal,
                    Message = string.IsNullOrEmpty(error.Message)
                        ? "action failed with an internal error"
                        : error.Message,
                });
        }

        if (job.IsCancellationRequested)
            return;
        try
        {
            await _sender.SendAsync(
                null,
                trace,
                new PluginEnvelope { JobUpdate = terminal },
                cancellationToken: job.Token).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (job.IsCancellationRequested)
        {
        }
    }

    private void CancelJob(PluginEnvelope envelope)
    {
        var jobId = envelope.CancelJob.JobId?.Value ?? "";
        if (!_jobs.TryGetValue(jobId, out var job))
            throw new InvalidDataException("host cancellation names no active job");
        if (!job.TryBeginHostCancellation(out var cancellation))
            throw new InvalidDataException("host repeated cancellation for an active job");
        TrackControlTask(AcknowledgeCancellationAsync(envelope, job, cancellation));
    }

    [SuppressMessage(
        "Design",
        "CA1031:Do not catch general exception types",
        Justification = "Job transport failures are reported by the single job-completion observer.")]
    private async Task AcknowledgeCancellationAsync(
        PluginEnvelope envelope,
        ActiveJob job,
        Task cancellation)
    {
        await IgnoreFailureAsync(cancellation).ConfigureAwait(false);
        try
        {
            await job.Task.ConfigureAwait(false);
        }
        catch
        {
            return; // The job completion observer reports transport failures.
        }
        if (_lifetimeToken.IsCancellationRequested)
            return;
        await _sender.SendAsync(
            envelope.MessageId,
            envelope.Trace,
            new PluginEnvelope
            {
                CancelJobAcknowledged = new CancelJobAcknowledged
                {
                    JobId = envelope.CancelJob.JobId.Clone(),
                },
            },
            cancellationToken: _lifetimeToken).ConfigureAwait(false);
    }

    private async Task ShutDownAsync(PluginEnvelope envelope)
    {
        if (Interlocked.Exchange(ref _stopping, 1) != 0)
            throw new InvalidDataException("host sent more than one shutdown request");
        var deadline = envelope.Shutdown.GracePeriodDeadline
            ?? throw new InvalidDataException("ShutdownRequest omitted its grace-period deadline");
        DateTimeOffset deadlineTime;
        try
        {
            deadlineTime = deadline.ToDateTimeOffset();
        }
        catch (ArgumentOutOfRangeException error)
        {
            throw new InvalidDataException("ShutdownRequest has an invalid grace-period deadline", error);
        }

        var jobs = _jobs.Values.ToArray();
        foreach (var job in jobs)
            Observe(job.CancelAsync());
        var completion = Task.WhenAll(jobs.Select(WaitForStoppedJobAsync));
        var remaining = deadlineTime - _timeProvider.GetUtcNow() - ShutdownAcknowledgementBudget;
        if (remaining > TimeSpan.Zero && !completion.IsCompleted)
        {
            try
            {
                await completion.WaitAsync(
                    remaining,
                    _timeProvider,
                    _lifetimeToken).ConfigureAwait(false);
            }
            catch (TimeoutException)
            {
            }
        }
        else if (completion.IsCompleted)
        {
            await completion.ConfigureAwait(false);
        }

        await _sender.SendAsync(
            envelope.MessageId,
            envelope.Trace,
            new PluginEnvelope { ShutdownAcknowledged = new ShutdownAcknowledged() },
            cancellationToken: _lifetimeToken).ConfigureAwait(false);
    }

    private void JobCompleted(string jobId, ActiveJob job, Task completed)
    {
        ((ICollection<KeyValuePair<string, ActiveJob>>)_jobs).Remove(
            new KeyValuePair<string, ActiveJob>(jobId, job));
        if (completed.IsFaulted && !_lifetimeToken.IsCancellationRequested)
            _fatalError.TrySetResult(completed.Exception!.GetBaseException());
        else if (completed.IsCanceled && !job.IsCancellationRequested
            && !_lifetimeToken.IsCancellationRequested)
            _fatalError.TrySetResult(new TaskCanceledException("plugin job ended unexpectedly"));
        Observe(job.MarkCompleted());
    }

    private void TrackControlTask(Task task)
    {
        var id = Interlocked.Increment(ref _nextControlTaskId);
        if (!_controlTasks.TryAdd(id, task))
            throw new InvalidOperationException("duplicate internal control-task ID");
        _ = task.ContinueWith(
            completed =>
            {
                _controlTasks.TryRemove(id, out _);
                if (completed.IsFaulted && !_lifetimeToken.IsCancellationRequested)
                    _fatalError.TrySetResult(completed.Exception!.GetBaseException());
            },
            CancellationToken.None,
            TaskContinuationOptions.ExecuteSynchronously,
            TaskScheduler.Default);
    }

    private void ValidateEnvelope(PluginEnvelope envelope)
    {
        if (envelope.MessageId == 0 || envelope.MessageId <= _lastHostMessageId)
            throw new InvalidDataException(
                "host message IDs must be nonzero and strictly increasing");
        if (envelope.SessionId != _sender.SessionId
            || envelope.PluginInstanceId != _sender.InstanceId)
            throw new InvalidDataException("host envelope belongs to another plugin instance");
        if (string.IsNullOrEmpty(envelope.Trace?.CorrelationId))
            throw new InvalidDataException("host omitted correlation context");
    }

    private void ValidateTrace(TraceContext trace)
        => ProtocolValidation.ValidateTrace(
            trace,
            _host.MaximumCallDepth,
            _host.MaximumCausalDepth);

    private static JobUpdate FailedUpdate(PluginJobId jobId, ProtocolError error)
        => new()
        {
            JobId = jobId.Clone(),
            State = JobState.Failed,
            Progress = 1,
            Error = error.Clone(),
        };

    private static async Task WaitForStoppedJobAsync(ActiveJob job)
    {
        await IgnoreFailureAsync(job.CancelAsync()).ConfigureAwait(false);
        await IgnoreFailureAsync(job.Task).ConfigureAwait(false);
    }

    [SuppressMessage(
        "Design",
        "CA1031:Do not catch general exception types",
        Justification = "Shutdown waits for completion but deliberately does not replace the shutdown acknowledgement with a job failure.")]
    private static async Task IgnoreFailureAsync(Task task)
    {
        try
        {
            await task.ConfigureAwait(false);
        }
        catch
        {
        }
    }

    private static void Observe(Task task)
    {
        if (task.IsCompleted)
        {
            _ = task.Exception;
            return;
        }
        _ = task.ContinueWith(
            completed => _ = completed.Exception,
            CancellationToken.None,
            TaskContinuationOptions.ExecuteSynchronously | TaskContinuationOptions.OnlyOnFaulted,
            TaskScheduler.Default);
    }

}
