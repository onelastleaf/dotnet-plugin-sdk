using Google.Protobuf.WellKnownTypes;
using Oll.Protocol;

namespace Onelastleaf.PluginSdk.Tests;

public sealed class ModelsTests
{
    [Fact]
    public void ActionResultOwnsItsMutableProtocolValue()
    {
        var input = new ConfigValue { StringValue = "original" };
        var result = new ActionResult(input);
        input.StringValue = "changed outside";
        var firstRead = result.Result!;
        firstRead.StringValue = "changed through getter";

        Assert.Equal("original", result.Result!.StringValue);
    }

    [Fact]
    public void ActionContextReturnsDefensiveTraceAndDeadlineCopies()
    {
        var writer = new TestClientStreamWriter();
        using var sender = new Sender(writer);
        var host = new Host(sender, Host.DefaultArtifactChunkBytes, 8, 8);
        var trace = TestProtocol.Trace();
        var deadline = Timestamp.FromDateTimeOffset(DateTimeOffset.UtcNow);
        var context = new ActionContext(
            "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
            deadline,
            trace,
            1,
            host,
            CancellationToken.None);

        context.Trace.TaskId = "mutated";
        context.Deadline!.Seconds = 0;

        Assert.Equal("task", context.Trace.TaskId);
        Assert.Equal(deadline, context.Deadline);
        host.Close();
    }

    [Fact]
    public void StructuredExceptionOwnsItsProtocolError()
    {
        var protocolError = new ProtocolError
        {
            Code = ErrorCode.NotFound,
            Message = "missing",
        };
        var exception = new ActionFailureException(protocolError);
        protocolError.Message = "changed outside";
        var firstRead = exception.Error;
        firstRead.Message = "changed through getter";

        Assert.Equal("missing", exception.Error.Message);
    }
}
