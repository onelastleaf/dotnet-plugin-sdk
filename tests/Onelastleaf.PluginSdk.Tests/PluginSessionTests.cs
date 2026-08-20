using Google.Protobuf.WellKnownTypes;
using Oll.Protocol;

namespace Onelastleaf.PluginSdk.Tests;

public sealed class PluginSessionTests
{
    [Fact]
    public async Task CancelJobDoesNotBlockHeartbeatWhileHandlerFinishes()
    {
        var started = NewSignal();
        var cancelled = NewSignal();
        var release = NewSignal();
        var actions = new Dictionary<string, RegisteredAction>
        {
            ["wait"] = new("wait", async (context, _) =>
            {
                using var registration = context.CancellationToken.Register(
                    () => cancelled.TrySetResult());
                started.TrySetResult();
                await release.Task;
                return new ActionResult();
            }),
        };
        var fixture = new SessionFixture(actions);
        using (fixture)
        {
            fixture.Incoming.Add(StartJob(1));
            Assert.Equal(
                PluginEnvelope.PayloadOneofCase.JobAccepted,
                (await fixture.Writer.ReadAsync()).PayloadCase);
            await started.Task.WaitAsync(TestTimeout.Value);

            fixture.Incoming.Add(CancelJob(2));
            fixture.Incoming.Add(Heartbeat(3));
            await cancelled.Task.WaitAsync(TestTimeout.Value);

            var heartbeat = await fixture.Writer.ReadAsync();
            Assert.Equal(PluginEnvelope.PayloadOneofCase.Heartbeat, heartbeat.PayloadCase);
            Assert.Equal(3ul, heartbeat.ReplyTo);

            release.TrySetResult();
            var acknowledged = await fixture.Writer.ReadAsync();
            Assert.Equal(
                PluginEnvelope.PayloadOneofCase.CancelJobAcknowledged,
                acknowledged.PayloadCase);
            Assert.Equal(2ul, acknowledged.ReplyTo);

            fixture.Incoming.Add(Shutdown(4, DateTimeOffset.UtcNow.AddSeconds(2)));
            Assert.Equal(
                PluginEnvelope.PayloadOneofCase.ShutdownAcknowledged,
                (await fixture.Writer.ReadAsync()).PayloadCase);
            await fixture.Session.WaitAsync(TestTimeout.Value);
        }
    }

    [Fact]
    public async Task ShutdownDeadlineIsHonouredWhenHandlerIgnoresCancellation()
    {
        var started = NewSignal();
        var release = NewSignal();
        var actions = new Dictionary<string, RegisteredAction>
        {
            ["ignore"] = new("ignore", async (_, _) =>
            {
                started.TrySetResult();
                await release.Task;
                return new ActionResult();
            }),
        };
        var fixture = new SessionFixture(actions);
        using (fixture)
        {
            fixture.Incoming.Add(StartJob(1, "ignore"));
            _ = await fixture.Writer.ReadAsync();
            await started.Task.WaitAsync(TestTimeout.Value);

            fixture.Incoming.Add(Shutdown(2, DateTimeOffset.UtcNow.AddSeconds(-1)));

            var acknowledged = await fixture.Writer.ReadAsync();
            Assert.Equal(
                PluginEnvelope.PayloadOneofCase.ShutdownAcknowledged,
                acknowledged.PayloadCase);
            await fixture.Session.WaitAsync(TestTimeout.Value);
            release.TrySetResult();
        }
    }

    [Fact]
    public async Task RepeatedCancellationCannotAccumulateControlTasks()
    {
        var started = NewSignal();
        var release = NewSignal();
        var actions = new Dictionary<string, RegisteredAction>
        {
            ["ignore"] = new("ignore", async (_, _) =>
            {
                started.TrySetResult();
                await release.Task;
                return new ActionResult();
            }),
        };
        var fixture = new SessionFixture(actions);
        using (fixture)
        {
            fixture.Incoming.Add(StartJob(1, "ignore"));
            _ = await fixture.Writer.ReadAsync();
            await started.Task.WaitAsync(TestTimeout.Value);

            fixture.Incoming.Add(CancelJob(2));
            fixture.Incoming.Add(CancelJob(3));

            var error = await Assert.ThrowsAsync<InvalidDataException>(() => fixture.Session);
            Assert.Contains("repeated cancellation", error.Message, StringComparison.Ordinal);
            release.TrySetResult();
        }
    }

    [Fact]
    public async Task AbnormalStreamClosureCancelsActiveJobs()
    {
        var started = NewSignal();
        var cancelled = NewSignal();
        var actions = new Dictionary<string, RegisteredAction>
        {
            ["cooperative"] = new("cooperative", async (context, _) =>
            {
                started.TrySetResult();
                try
                {
                    await Task.Delay(Timeout.InfiniteTimeSpan, context.CancellationToken);
                }
                finally
                {
                    cancelled.TrySetResult();
                }
                return new ActionResult();
            }),
        };
        var fixture = new SessionFixture(actions);
        using (fixture)
        {
            fixture.Incoming.Add(StartJob(1, "cooperative"));
            _ = await fixture.Writer.ReadAsync();
            await started.Task.WaitAsync(TestTimeout.Value);

            fixture.Incoming.Complete();

            await Assert.ThrowsAsync<IOException>(() => fixture.Session);
            await cancelled.Task.WaitAsync(TestTimeout.Value);
        }
    }

    [Fact]
    public async Task StructuredActionFailureIsPreservedInTerminalUpdate()
    {
        var failure = new ProtocolError
        {
            Code = ErrorCode.FailedPrecondition,
            Message = "configuration is incomplete",
        };
        failure.Metadata.Add("setting", "endpoint");
        var actions = new Dictionary<string, RegisteredAction>
        {
            ["fail"] = new("fail", (_, _) => throw new ActionFailureException(failure)),
        };
        var fixture = new SessionFixture(actions);
        using (fixture)
        {
            fixture.Incoming.Add(StartJob(1, "fail"));
            _ = await fixture.Writer.ReadAsync();

            var terminal = await fixture.Writer.ReadAsync();
            Assert.Equal(PluginEnvelope.PayloadOneofCase.JobUpdate, terminal.PayloadCase);
            Assert.Equal(JobState.Failed, terminal.JobUpdate.State);
            Assert.Equal(ErrorCode.FailedPrecondition, terminal.JobUpdate.Error.Code);
            Assert.Equal("endpoint", terminal.JobUpdate.Error.Metadata["setting"]);

            fixture.Incoming.Add(Shutdown(2, DateTimeOffset.UtcNow.AddSeconds(2)));
            _ = await fixture.Writer.ReadAsync();
            await fixture.Session.WaitAsync(TestTimeout.Value);
        }
    }

    private static TaskCompletionSource NewSignal()
        => new(TaskCreationOptions.RunContinuationsAsynchronously);

    private static PluginEnvelope StartJob(ulong messageId, string action = "wait")
    {
        var envelope = TestProtocol.HostEnvelope(messageId);
        envelope.StartJob = new StartJobRequest
        {
            JobId = new PluginJobId
            {
                Value = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
            },
            Action = new ActionInvocation { Action = action },
        };
        return envelope;
    }

    private static PluginEnvelope CancelJob(ulong messageId)
    {
        var envelope = TestProtocol.HostEnvelope(messageId);
        envelope.CancelJob = new CancelJobRequest
        {
            JobId = new PluginJobId
            {
                Value = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
            },
            Reason = JobCancellationReason.UserRequest,
        };
        return envelope;
    }

    private static PluginEnvelope Heartbeat(ulong messageId)
    {
        var envelope = TestProtocol.HostEnvelope(messageId);
        envelope.Heartbeat = new Heartbeat { Nonce = messageId };
        return envelope;
    }

    private static PluginEnvelope Shutdown(ulong messageId, DateTimeOffset deadline)
    {
        var envelope = TestProtocol.HostEnvelope(messageId);
        envelope.Shutdown = new ShutdownRequest
        {
            GracePeriodDeadline = Timestamp.FromDateTimeOffset(deadline),
            Reason = "test complete",
        };
        return envelope;
    }

    private sealed class SessionFixture : IDisposable
    {
        internal SessionFixture(IReadOnlyDictionary<string, RegisteredAction> actions)
        {
            Sender = new Sender(Writer);
            Sender.SetIdentity(TestProtocol.SessionId, TestProtocol.InstanceId);
            Host = new Host(Sender, Host.DefaultArtifactChunkBytes, 8, 8);
            var session = new PluginSession(Incoming, Sender, Host, actions);
            Session = session.RunAsync(0);
        }

        internal TestAsyncStreamReader Incoming { get; } = new();
        internal TestClientStreamWriter Writer { get; } = new();
        internal Sender Sender { get; }
        internal Host Host { get; }
        internal Task Session { get; }

        public void Dispose()
        {
            Host.Close();
            Sender.Dispose();
        }
    }
}
