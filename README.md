# onelastleaf .NET plugin SDK

Build trusted [onelastleaf](https://github.com/onelastleaf/onelastleaf) process
plugins in C# without having to implement the gRPC session protocol yourself.
The NuGet package is `Onelastleaf.PluginSdk`.

The SDK handles the connection, handshake, concurrent jobs, cancellation,
shutdown, logging, configuration calls, and artifact transfer. Your plugin
registers actions and implements what those actions should do.

## Build and test this repository

You need the .NET 10 SDK to build the whole repository. The library itself
targets both .NET 8 and .NET 10, while its test project targets .NET 10.

There is no solution file, so run the commands against the project files:

```sh
dotnet build src/Onelastleaf.PluginSdk/Onelastleaf.PluginSdk.csproj \
  --configuration Release
dotnet test tests/Onelastleaf.PluginSdk.Tests/Onelastleaf.PluginSdk.Tests.csproj \
  --configuration Release
```

You can also compile the host-driven conformance plugin and create a NuGet
package locally:

```sh
dotnet build examples/Conformance/Conformance.csproj --configuration Release
dotnet pack src/Onelastleaf.PluginSdk/Onelastleaf.PluginSdk.csproj \
  --configuration Release --output artifacts
```

The package is written to `artifacts/Onelastleaf.PluginSdk.0.1.0.nupkg`.
`examples/Conformance` is not a standalone command-line program: oll must launch
it and provide a protocol session, so running it directly will report that
`OLL_PLUGIN_ENDPOINT` is missing.

## Create a plugin

The easiest starting point is oll's project generator. It creates the C#
project, a small test, and the `oll.toml` recipe that tells oll how to publish
and launch the plugin:

```sh
oll plugin new hello-plugin \
  --language dotnet \
  --id dev.example.hello \
  --name hello-plugin
cd hello-plugin
dotnet test tests/Plugin.Tests.csproj
```

The generated project depends on `Onelastleaf.PluginSdk` version `0.1.0`. A
minimal plugin looks like this:

```csharp
using Onelastleaf.PluginSdk;

var plugin = Plugin.Create("dev.example.hello", "0.1.0")
    .Action("echo", "Return the supplied arguments", (_, arguments) =>
        Task.FromResult(ActionResult.String(string.Join(" ", arguments))));

await plugin.RunAsync();
```

The ID passed to `Plugin.Create` is permanent and must match `plugin.id` in
`oll.toml`. Action handlers receive an `ActionContext`, the ordered string
arguments from `oll plugin call`, and a cancellation token at
`context.CancellationToken`. Return an `ActionResult` when the job is finished.

Do not set `OLL_PLUGIN_ENDPOINT` or start a gRPC server in your plugin. oll owns
the loopback server, starts the plugin process, supplies that environment
variable, and uses the plugin's standard input as a parent-liveness pipe.
`Plugin.RunAsync()` takes care of the client side of that contract.

### Try local SDK changes in a generated plugin

While changing this SDK, you can temporarily replace the generated plugin's
`PackageReference` with a `ProjectReference`:

```xml
<ItemGroup>
  <ProjectReference
    Include="../../dotnet-plugin-sdk/src/Onelastleaf.PluginSdk/Onelastleaf.PluginSdk.csproj" />
</ItemGroup>
```

This example assumes `hello-plugin` and `dotnet-plugin-sdk` are sibling
directories. Adjust the path relative to `hello-plugin/src/Plugin.csproj` for
your checkout. This is useful for `dotnet build` and `dotnet test`, but remember
that oll installs from a fresh Git checkout. A reference to a sibling directory
will not exist there. Before installing the plugin, use the published
`PackageReference`, or make the package from `dotnet pack` available through a
NuGet source that the checked-out plugin can resolve.

## Install and call the plugin through oll

Commit the generated plugin to a Git repository and push it to a remote that
oll can clone. With an initialized oll node already running, install the source,
start the plugin, and call its `echo` action:

```sh
oll status
oll plugin install https://github.com/your-name/hello-plugin.git --source
oll plugin start dev.example.hello
oll plugin call dev.example.hello echo hello from dotnet
```

`oll plugin call` prints the job ID. Use it to inspect the result, or read the
plugin's captured standard output and error:

```sh
oll job info <job-id>
oll plugin log dev.example.hello
```

If you have not set up oll yet, follow the main project's
[quick start](https://github.com/onelastleaf/onelastleaf#quick-start) first.
`oll plugin install` accepts Git, HTTP(S), SSH, and SCP-style Git remotes; it
does not install directly from a local working-directory path.

## Calling back into oll

An action can read its current oll-managed configuration, invoke a configured
Lua function, read documents, write structured logs, and store verified
artifacts through `ActionContext` and `context.Host`. For example:

```csharp
using Oll.Protocol;
using Onelastleaf.PluginSdk;

var plugin = Plugin.Create("dev.example.hello", "0.1.0")
    .Action("configured", "Return this plugin's configuration", async (context, _) =>
    {
        var configured = await context.GetConfigAsync();
        await context.Host.LogAsync(
            context.Trace,
            LogLevel.Info,
            "hello-plugin",
            "configuration loaded",
            cancellationToken: context.CancellationToken);
        return new ActionResult(configured.Value);
    });

await plugin.RunAsync();
```

Configuration stays in oll; it is fetched when the action asks for it rather
than copied into the plugin as an arbitrary file. The generated protobuf types
used by these calls are available in the `Oll.Protocol` namespace. See
[`examples/Conformance/Program.cs`](examples/Conformance/Program.cs) for working
examples of configuration functions, document reads, structured logs,
cancellation, and artifact transfer.

## Runtime model in one minute

- Plugins are trusted, independent child processes.
- oll hosts the loopback gRPC server; the plugin connects as a client.
- Closing the plugin's standard input means its parent is gone, so the SDK exits.
- One plugin process may run several jobs at the same time.
- Job cancellation cancels only that job. Plugin shutdown is a separate request.
- Plugin standard output and error are captured in its per-plugin log.

These details mostly stay out of application code, but they explain why a
plugin should always enter through `Plugin.RunAsync()` and be exercised through
oll rather than launched by hand.

## License

[GPL-3.0-only](LICENSE)
