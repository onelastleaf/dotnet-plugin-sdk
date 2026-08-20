# onelastleaf .NET plugin SDK

This is the official C# SDK for trusted
[onelastleaf](https://github.com/onelastleaf/onelastleaf) process plugins. You
write ordinary async action handlers; the SDK takes care of the gRPC handshake,
concurrent jobs, cancellation, shutdown, configuration calls, logging, and
verified artifact transfer.

The NuGet package is `Onelastleaf.PluginSdk`.

## Build and test this SDK

Install the .NET 10 SDK and the .NET 8 runtime, then run the repository-wide
checks from its root:

```sh
dotnet restore dotnet-plugin-sdk.slnx --locked-mode
dotnet format dotnet-plugin-sdk.slnx --no-restore --verify-no-changes
dotnet build dotnet-plugin-sdk.slnx --configuration Release --no-restore
dotnet test tests/Onelastleaf.PluginSdk.Tests/Onelastleaf.PluginSdk.Tests.csproj \
  --configuration Release --no-build
```

The library targets both .NET 8 and .NET 10, and the test suite runs against
both targets. The conformance plugin targets .NET 10, which is why building the
complete repository needs the .NET 10 SDK. CI runs the same format, build, test,
and package checks with locked NuGet dependencies. If you have the .NET 10 SDK
but not the .NET 8 runtime locally, use
`dotnet test ... --framework net10.0`; the solution build still compiles the
library for net8.0.

To produce a package locally:

```sh
dotnet pack src/Onelastleaf.PluginSdk/Onelastleaf.PluginSdk.csproj \
  --configuration Release --no-build --output artifacts
```

The result is `artifacts/Onelastleaf.PluginSdk.0.1.0.nupkg`. The program under
`examples/Conformance` is host-driven, not a standalone CLI. If you run it by
hand, it will correctly report that `OLL_PLUGIN_ENDPOINT` is missing.

## Create your first plugin

Let oll generate the project, tests, and `oll.toml` recipe together:

```sh
oll plugin new hello-plugin \
  --language dotnet \
  --id dev.example.hello \
  --name hello-plugin
cd hello-plugin
dotnet test tests/Plugin.Tests.csproj
```

The generated project references `Onelastleaf.PluginSdk` version `0.1.0`. Its
entry point follows this shape:

```csharp
using Onelastleaf.PluginSdk;

var plugin = Plugin.Create("dev.example.hello", "0.1.0")
    .Action("echo", "Return the supplied arguments", (_, arguments) =>
        Task.FromResult(ActionResult.FromString(string.Join(" ", arguments))));

await plugin.RunAsync();
```

`Plugin.Create` takes the permanent publisher ID from `oll.toml`, not the
mutable display name. Register every action before calling `RunAsync`; one
`Plugin` instance represents one process session and cannot be run twice.

Do not start a gRPC server or set `OLL_PLUGIN_ENDPOINT` yourself. oll owns an
ephemeral loopback server, starts the plugin process, and supplies the endpoint.
The plugin connects as the gRPC client. Its standard input is also a liveness
pipe: EOF means the parent is gone, so the SDK cancels the session and exits.

### Use an SDK checkout while developing

To try local SDK changes, temporarily replace the generated package reference
with a project reference:

```xml
<ItemGroup>
  <ProjectReference
    Include="../../dotnet-plugin-sdk/src/Onelastleaf.PluginSdk/Onelastleaf.PluginSdk.csproj" />
</ItemGroup>
```

That path assumes `hello-plugin` and `dotnet-plugin-sdk` are sibling folders and
the reference lives in `hello-plugin/src/Plugin.csproj`. Adjust it for your
layout.

This is only a local development setup. oll installs a plugin from a fresh Git
checkout, where a sibling project reference will not exist. Before installing,
switch back to the published `PackageReference`, or publish the locally packed
NuGet package through a source that the clean checkout can resolve.

## Install and call it through oll

Commit the generated plugin to a Git repository that oll can clone. With an oll
node already initialized and running:

```sh
oll status
oll plugin install https://github.com/your-name/hello-plugin.git --source
oll plugin start dev.example.hello
oll plugin call dev.example.hello echo -- hello from dotnet
```

The call returns after oll and the plugin have admitted the job, and prints its
job ID. Inspect the eventual result and plugin log separately:

```sh
oll job info <job-id>
oll plugin log dev.example.hello
```

A new installation starts in the stopped state, and a call never starts it
implicitly. `plugin start` persists the desired running state. If oll itself is
not set up yet, follow its
[quick start](https://github.com/onelastleaf/onelastleaf#quick-start) first.

## Work inside an action

Each handler receives an `ActionContext`, the exact ordered argument list, and
an action-scoped cancellation token. Host capabilities live directly on the
context, so application code does not need to pass raw trace state around:

```csharp
using Oll.Protocol;
using Onelastleaf.PluginSdk;

var plugin = Plugin.Create("dev.example.hello", "0.1.0")
    .Action("configured", "Read current plugin configuration", async (context, _) =>
    {
        var configured = await context.GetConfigAsync();
        await context.LogAsync(
            LogLevel.Info,
            "hello-plugin",
            "configuration loaded");
        return new ActionResult(configured.Value);
    });

await plugin.RunAsync();
```

Configuration remains authoritative in oll and is fetched when the handler asks
for it. `GetConfigAsync`, `InvokeConfigFunctionAsync`, and `HostCallAsync` all
create a correctly nested trace and enforce the negotiated call-depth limit.
The generated request and response types are in `Oll.Protocol`.

Respect `context.CancellationToken` in every cancellable operation:

```csharp
.Action("wait", "Wait until cancelled", async (context, _) =>
{
    await Task.Delay(Timeout.InfiniteTimeSpan, context.CancellationToken);
    return new ActionResult();
})
```

Cancellation is job-scoped. It must not stop other jobs running in the same
plugin process. The SDK keeps the control stream responsive while a cancelled
handler finishes. .NET cannot forcibly stop arbitrary managed code, so a handler
that ignores cancellation may continue until oll's process-level shutdown
deadline ends the process.

### Return an expected failure

Throw `ActionFailureException` when a failure is part of the action's domain,
rather than an SDK or programming error:

```csharp
throw new ActionFailureException(new ProtocolError
{
    Code = ErrorCode.FailedPrecondition,
    Message = "the plugin endpoint is not configured",
});
```

The SDK preserves that structured error in the terminal job update. An
unhandled exception is still caught at the action boundary and reported as
`INTERNAL`; it does not escape as an unobserved background-task failure.

### Store and return an artifact

An artifact is hashed and size-checked before its first byte is sent. Pass a
readable, seekable stream so the SDK can perform that validation without keeping
the whole artifact in memory:

```csharp
using System.Security.Cryptography;
using Google.Protobuf;
using Oll.Protocol;

var bytes = "artifact payload"u8.ToArray();
var descriptor = new ArtifactDescriptor
{
    ArtifactId = new PluginArtifactId
    {
        Value = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
    },
    FileName = "result.txt",
    MediaType = "text/plain",
    SizeBytes = (ulong)bytes.Length,
    Sha256 = ByteString.CopyFrom(SHA256.HashData(bytes)),
};

using var content = new MemoryStream(bytes, writable: false);
var stored = await context.StoreArtifactAsync(descriptor, content);
return new ActionResult(ActionResult.FromString("done").Result, [stored]);
```

The default chunk size automatically respects the host's negotiated limit; you
can pass a smaller `chunkSize` when useful. Empty artifacts are valid. Only the
opaque `StoredArtifact` returned after oll acknowledges storage can be included
in `ActionResult`, which prevents a terminal result from claiming an artifact
that was never stored.

See [`examples/Conformance/Program.cs`](examples/Conformance/Program.cs) for one
plugin that exercises configuration functions, document reads, structured logs,
cancellation, and artifact transfer.

## Protocol and runtime guarantees

- Plugins are trusted independent child processes.
- oll hosts the loopback gRPC server; the plugin is always the client.
- One process can run several jobs concurrently.
- `CancelJobRequest` affects one job; `ShutdownRequest` affects the process.
- Parent stdin EOF is a mandatory exit signal.
- Plugin stdout and stderr go to its per-plugin log.
- Incoming and outgoing envelopes are limited to 64 MiB.
- Stalled outgoing writes have a bounded admission queue.

The SDK follows protobuf wire compatibility directly. It does not compute,
publish, or compare a schema fingerprint: compatible changes preserve field
numbers and wire types, give additions safe absent semantics, and tolerate
unknown fields. Pinning an SDK package gives a reproducible build; it is not a
replacement for protobuf compatibility rules.

## License

[GPL-3.0-only](LICENSE)
