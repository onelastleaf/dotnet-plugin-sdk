using Google.Protobuf;
using Oll.Protocol;
using Onelastleaf.PluginSdk;

var plugin = Plugin.Create("org.onelastleaf.conformance", "0.1.0")
    .Action("echo", "Echo arguments", (context, arguments) =>
        Task.FromResult(ActionResult.FromString(string.Join(" ", arguments))))
    .Action("wait", "Wait for cancellation", async (context, arguments) =>
    {
        await Task.Delay(Timeout.InfiniteTimeSpan, context.CancellationToken);
        return new ActionResult();
    })
    .Action("host", "Exercise host capabilities", async (context, arguments) =>
    {
        var configured = await context.GetConfigAsync();
        if (configured.Value?.KindCase != ConfigValue.KindOneofCase.FunctionValue)
            throw new InvalidOperationException("GetConfig omitted function");
        var invoked = await context.InvokeConfigFunctionAsync(
            configured.Value.FunctionValue,
            [new ConfigValue { StringValue = "config" }]);
        if (invoked.Results.Count != 1
            || invoked.Results[0].KindCase != ConfigValue.KindOneofCase.StringValue)
            throw new InvalidOperationException("configuration function omitted string result");
        var read = await context.HostCallAsync(new HostCallRequest
        {
            ReadDocument = new ReadDocumentRequest
            {
                Path = new DocumentPath { Value = "/conformance.md" },
                Projection = DocumentProjection.Content,
            },
        });
        if (read.ResultCase != HostCallResponse.ResultOneofCase.ReadDocument
            || read.ReadDocument.Document?.RepresentationCase
                != DocumentSnapshot.RepresentationOneofCase.Content)
            throw new InvalidOperationException("document call omitted text content");
        await context.LogAsync(
            LogLevel.Info,
            "conformance",
            "host action complete");
        return ActionResult.FromString(
            $"{invoked.Results[0].StringValue}|{read.ReadDocument.Document.Content}");
    })
    .Action("artifact", "Exercise artifact transfer", async (context, arguments) =>
    {
        var descriptor = new ArtifactDescriptor
        {
            ArtifactId = new PluginArtifactId
            {
                Value = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
            },
            FileName = "conformance.txt",
            MediaType = "text/plain",
            SizeBytes = 16,
            Sha256 = ByteString.CopyFrom(Convert.FromHexString(
                "a11a4045c89f727fadb9aeddb0f29637ce5b505846afebd82ae2c01b6733a6b5")),
        };
        using var content = new MemoryStream("artifact payload"u8.ToArray(), writable: false);
        var stored = await context.StoreArtifactAsync(descriptor, content);
        return new ActionResult(ActionResult.FromString("artifact").Result, [stored]);
    });

await plugin.RunAsync();
