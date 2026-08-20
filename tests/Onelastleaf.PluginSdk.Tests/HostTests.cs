using System.Security.Cryptography;
using Google.Protobuf;
using Oll.Protocol;

namespace Onelastleaf.PluginSdk.Tests;

public sealed class HostTests
{
    [Fact]
    public async Task CancelledRequestConsumesItsOneLateResponse()
    {
        var (host, writer, sender) = CreateHost();
        using (sender)
        using (var cancellation = new CancellationTokenSource())
        {
            var call = host.GetConfigAsync(
                TestProtocol.Trace(),
                cancellationToken: cancellation.Token);
            var request = await writer.ReadAsync();
            await cancellation.CancelAsync();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => call);

            var exception = Record.Exception(() => host.Route(Reply(
                request,
                new PluginEnvelope
                {
                    HostResult = new HostCallResponse { GetConfig = new GetConfigResponse() },
                })));

            Assert.Null(exception);
            host.Close();
        }
    }

    [Fact]
    public async Task ResponseMustPreserveTheEntireTraceContext()
    {
        var (host, writer, sender) = CreateHost();
        using (sender)
        {
            var call = host.GetConfigAsync(TestProtocol.Trace());
            var request = await writer.ReadAsync();
            var changedTrace = request.Trace.Clone();
            changedTrace.TaskId = "another-task";

            Assert.Throws<InvalidDataException>(() => host.Route(Reply(
                request,
                new PluginEnvelope
                {
                    Trace = changedTrace,
                    HostResult = new HostCallResponse { GetConfig = new GetConfigResponse() },
                })));

            var error = await Assert.ThrowsAsync<InvalidDataException>(() => call);
            Assert.Contains("trace context", error.Message, StringComparison.Ordinal);
            host.Close();
        }
    }

    [Fact]
    public async Task ClosingSessionFailsPendingCallsAndRejectsNewOnes()
    {
        var (host, writer, sender) = CreateHost();
        using (sender)
        {
            var pending = host.GetConfigAsync(TestProtocol.Trace());
            _ = await writer.ReadAsync();
            var closed = new IOException("connection failed");

            host.Close(closed);

            Assert.Same(closed, await Assert.ThrowsAsync<IOException>(() => pending));
            await Assert.ThrowsAsync<InvalidOperationException>(() =>
                host.GetConfigAsync(TestProtocol.Trace("new-call")));
        }
    }

    [Fact]
    public async Task FailedWriteUnregistersRequestThatNeverReachedTheStream()
    {
        var writer = new TestClientStreamWriter(_ =>
            Task.FromException(new IOException("write failed")));
        using var sender = new Sender(writer);
        sender.SetIdentity(TestProtocol.SessionId, TestProtocol.InstanceId);
        var host = new Host(sender, Host.DefaultArtifactChunkBytes, 8, 8);

        await Assert.ThrowsAsync<IOException>(() =>
            host.GetConfigAsync(TestProtocol.Trace()));

        var late = new PluginEnvelope
        {
            MessageId = 100,
            ReplyTo = 1,
            SessionId = TestProtocol.SessionId,
            PluginInstanceId = TestProtocol.InstanceId,
            Trace = TestProtocol.Trace(),
            HostResult = new HostCallResponse { GetConfig = new GetConfigResponse() },
        };
        Assert.Throws<InvalidDataException>(() => host.Route(late));
        host.Close();
    }

    [Fact]
    public async Task EmptyArtifactUsesZeroChunksAndCanBeReturnedOnlyAfterStorageAck()
    {
        var (host, writer, sender) = CreateHost();
        using (sender)
        using (var content = new MemoryStream())
        {
            var descriptor = Descriptor([]);
            var storing = host.StoreArtifactAsync(
                TestProtocol.Trace(),
                "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
                descriptor,
                content);
            var start = await writer.ReadAsync();
            Assert.Equal(0u, start.ArtifactStart.ChunkCount);
            host.Route(Reply(
                start,
                new PluginEnvelope
                {
                    ArtifactAccepted = new ArtifactTransferAccepted
                    {
                        ArtifactId = descriptor.ArtifactId.Clone(),
                    },
                }));
            var complete = await writer.ReadAsync();
            Assert.Equal(PluginEnvelope.PayloadOneofCase.ArtifactComplete, complete.PayloadCase);
            host.Route(Reply(
                complete,
                new PluginEnvelope
                {
                    ArtifactStored = new ArtifactStored
                    {
                        ArtifactId = descriptor.ArtifactId.Clone(),
                    },
                }));

            var stored = await storing;
            Assert.Equal(descriptor, stored.Descriptor);
            host.Close();
        }
    }

    [Fact]
    public async Task ArtifactIsVerifiedThenStreamedInBoundedChunks()
    {
        var (host, writer, sender) = CreateHost(maximumChunkBytes: 3);
        using (sender)
        using (var content = new MemoryStream("payload"u8.ToArray(), writable: false))
        {
            var descriptor = Descriptor("payload"u8.ToArray());
            var storing = host.StoreArtifactAsync(
                TestProtocol.Trace(),
                "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
                descriptor,
                content);
            var start = await writer.ReadAsync();
            Assert.Equal(3u, start.ArtifactStart.ChunkCount);
            host.Route(Reply(
                start,
                new PluginEnvelope
                {
                    ArtifactAccepted = new ArtifactTransferAccepted
                    {
                        ArtifactId = descriptor.ArtifactId.Clone(),
                    },
                }));

            var chunks = new List<byte>();
            for (uint index = 0; index < 3; index++)
            {
                var chunk = await writer.ReadAsync();
                Assert.Equal(index, chunk.ArtifactChunk.ChunkIndex);
                Assert.InRange(chunk.ArtifactChunk.Data.Length, 1, 3);
                chunks.AddRange(chunk.ArtifactChunk.Data);
            }
            Assert.Equal("payload"u8.ToArray(), chunks.ToArray());
            var complete = await writer.ReadAsync();
            host.Route(Reply(
                complete,
                new PluginEnvelope
                {
                    ArtifactStored = new ArtifactStored
                    {
                        ArtifactId = descriptor.ArtifactId.Clone(),
                    },
                }));

            _ = await storing;
            host.Close();
        }
    }

    [Fact]
    public async Task ArtifactRequiresSeekabilityBeforeStartingTransfer()
    {
        var (host, _, sender) = CreateHost();
        using (sender)
        using (var content = new NonSeekableReadStream("payload"u8.ToArray()))
        {
            await Assert.ThrowsAsync<ArgumentException>(() => host.StoreArtifactAsync(
                TestProtocol.Trace(),
                "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
                Descriptor("payload"u8.ToArray()),
                content));
            host.Close();
        }
    }

    [Fact]
    public async Task ArtifactProtocolErrorRetainsStructuredHostFailure()
    {
        var (host, writer, sender) = CreateHost();
        using (sender)
        using (var content = new MemoryStream())
        {
            var storing = host.StoreArtifactAsync(
                TestProtocol.Trace(),
                "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
                Descriptor([]),
                content);
            var start = await writer.ReadAsync();
            host.Route(Reply(
                start,
                new PluginEnvelope
                {
                    ProtocolError = new ProtocolError
                    {
                        Code = ErrorCode.PayloadTooLarge,
                        Message = "policy limit",
                        Retryable = false,
                    },
                }));

            var error = await Assert.ThrowsAsync<HostProtocolException>(() => storing);
            Assert.Equal(ErrorCode.PayloadTooLarge, error.Error.Code);
            Assert.Equal("policy limit", error.Error.Message);
            host.Close();
        }
    }

    private static (Host Host, TestClientStreamWriter Writer, Sender Sender) CreateHost(
        ulong maximumChunkBytes = Host.DefaultArtifactChunkBytes)
    {
        var writer = new TestClientStreamWriter();
        var sender = new Sender(writer);
        sender.SetIdentity(TestProtocol.SessionId, TestProtocol.InstanceId);
        return (new Host(sender, maximumChunkBytes, 8, 8), writer, sender);
    }

    private static ArtifactDescriptor Descriptor(byte[] content)
        => new()
        {
            ArtifactId = new PluginArtifactId
            {
                Value = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
            },
            FileName = "artifact.bin",
            MediaType = "application/octet-stream",
            SizeBytes = (ulong)content.Length,
            Sha256 = ByteString.CopyFrom(SHA256.HashData(content)),
        };

    private static PluginEnvelope Reply(PluginEnvelope request, PluginEnvelope response)
    {
        response.MessageId = request.MessageId + 100;
        response.ReplyTo = request.MessageId;
        response.SessionId = request.SessionId;
        response.PluginInstanceId = request.PluginInstanceId;
        response.Trace ??= request.Trace.Clone();
        return response;
    }
}
