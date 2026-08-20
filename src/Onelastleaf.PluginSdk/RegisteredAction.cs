namespace Onelastleaf.PluginSdk;

internal sealed record RegisteredAction(
    string Description,
    Func<ActionContext, IReadOnlyList<string>, Task<ActionResult>> Handler);
