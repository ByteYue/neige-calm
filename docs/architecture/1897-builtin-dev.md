## Outcome

Extract development tooling into a compiled-in plugin component using the existing registry, lifecycle, template ownership and MCP authorization. No persistent Git plugin process is required.

## Boundaries

- Preserve `dev.neige.git-forge`, `calm.track.publish` and `calm.review.round` for saved template compatibility. Display the component as `development`.
- Only the compiled catalog supplies native code. Disk installation, reload and remote connectors cannot impersonate its identity.
- Keep Git lowering in the Dev component and execution through kernel recorded operations. Generic task lifecycle, ratification, Report consistency, role checks, session admission and forge trust remain kernel responsibilities.
- Built-in components use the shared enable, disable and configuration lifecycle without a PID, process token or supervisor. A fresh installation registers Dev disabled; upgrades preserve user configuration and enabled state and revoke old process tokens.
- The Issue template has a fixed Dev owner. Other plugins cannot claim it, including while Dev is disabled. New Issue creation keeps existing owner, trust and input requirements; saved snapshots and legacy ownership remain unchanged.

## Plugin documentation references

`@` offers installed plugins using the existing read-only plugin catalog, including disabled plugins. A pick displays a short name as a composer token and sends the plugin ID, name and brief description as ordinary user message text. Plugin descriptions are documentation, not developer instructions or authority. Existing report, tag and block mentions retain their own access boundary.

Issue development always includes the compiled Dev guide through the saved template identity or owner, independently of enablement. Other templates do not receive that guide automatically. References never install, enable, configure or authorize plugins. No Track or conversation selection API, grant table or sidebar management is introduced.

Tool discovery and invocation keep the existing kernel scope, role, trust, live-plugin and session checks. An otherwise eligible Planner calling an exact known tool from a disabled plugin receives a readable enablement error. Unknown tools, out-of-scope callers and other unauthorized identities retain their existing rejection behavior.

## Acceptance

1. Built-in lifecycle has no child PID, token or supervisor and preserves upgrade configuration.
2. Reserved identities cannot be replaced from disk; external backends retain their behavior.
3. Disabled plugin descriptions can be referenced and sent through the actual composer without lifecycle writes or authorization changes.
4. Issue instructions always contain the Dev guide, including after disabling Dev and for legacy unbound Issue templates, without rewriting snapshots, input or ownership.
5. Discovery and calls continue to enforce kernel permissions; stale calls cannot use a disabled component, and disabled hints do not disclose tools to unauthorized roles.
6. The frontend exposes references in the existing input menu and neutral tokens, with no plugin management in Cards or the sidebar.
7. Regenerate affected contracts, pass relevant tests and gates, preview in Chromium, and converge two independent reviews before delivery.

## Compatibility

Built-in code updates through releases; enablement remains dynamic. No shared-library loader or new plugin framework is added. Released migrations remain frozen; this change needs no database migration. REST revision 16 and web compatibility 35 cover required `can_uninstall` metadata for the built-in lifecycle. The existing plugin catalog supplies reference descriptions without a new wire contract.

The Rust terminology baseline increase of 31 reflects relocated Git payload construction and tests, rather than new vocabulary or a weakened gate.


The new-track composer displays the selected template's default plugin documentation as locked neutral pills. The read-only `/api/track-templates/{id}/plugin-guides` view uses the compiled catalog's template ownership, independently of enablement; it exposes only ID and display name. Frontend composition does not duplicate template/plugin mappings or alter the first message, plugin state, owner or permissions. Switching away from the template removes its inherited indicators.
