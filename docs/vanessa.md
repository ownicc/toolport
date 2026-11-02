# Vanessa Automation across Enterprise restarts

Vanessa's MCP endpoint runs with the 1C:Enterprise session. Updating the database
and closing that session can make `http://127.0.0.1:9876/mcp` unavailable. Connect
Codex to Toolport over stdio so the client-facing connection survives the outage.
Start Enterprise with Vanessa's MCP enabled again after the update; Toolport
reconnects to the same URL and initializes a fresh MCP session.

## Setup from source

Build the headless gateway (no desktop shell or JavaScript dependencies needed):

```sh
cargo build --locked --manifest-path src-tauri/Cargo.toml \
  --no-default-features --features search-static --bin toolport-gateway
```

Copy [the Vanessa registry example](../data/vanessa.registry.json.example) into
a dedicated persistent data directory, for example
`/home/you/.local/state/toolport-vanessa/registry.json`. Keep this directory across
restarts: Toolport stores its learned catalog there. Adjust the URL if Vanessa
uses another port. This setup does not require credentials for the loopback
endpoint.

Replace the project's direct Vanessa MCP entry in `.codex/config.toml` with:

```toml
[mcp_servers.toolport]
command = "/absolute/path/to/toolport/src-tauri/target/debug/toolport-gateway"
startup_timeout_sec = 30
tool_timeout_sec = 120

[mcp_servers.toolport.env]
TOOLPORT_DATA_DIR = "/home/you/.local/state/toolport-vanessa"
TOOLPORT_REGISTRY = "/home/you/.local/state/toolport-vanessa/registry.json"
TOOLPORT_CLIENT_ID = "codex"
TOOLPORT_DISCOVERY = "full"
```

Use absolute paths and start a new Codex session to use the changed configuration.
The explicit `TOOLPORT_CLIENT_ID` matters: it enables Codex's search/call fallback
in Full mode. An MCP `clientInfo.name` alone does not select that adapter identity.

## During an update

Keep the Codex session running while Enterprise is closed. Toolport keeps its
last catalog and retries failed connections with backoff. Calls during downtime
report unavailability. Once Enterprise and Vanessa MCP are running again,
subsequent calls resume; repeated rapid calls do not bypass the retry delay.
See [downstream lifecycle](configuration.md#downstream-lifecycle) for the timing.
Exhausted connection attempts put an idle endpoint back into background recovery,
including when it was connected successfully before the outage. Other active
calls keep their connection. The first call after a restart may report an invalid
old MCP session; later calls use the newly initialized session.

Tools already discovered appear as `vanessa__get_environment_data`, etc. If
Vanessa was unavailable at the first discovery, or its tool set changed, use
the permanent helpers without restarting Codex:

1. Call `toolport_status` to see connection/retry state.
2. Call `toolport_search_tools` with
   `{"query":"get_environment_data","server":"vanessa"}`.
3. Call `toolport_call_tool` with
   `{"name":"vanessa__get_environment_data","arguments":{}}`.

A first-ever offline start has no Vanessa catalog yet; the helpers remain
available and reach the live catalog after Vanessa starts. Toolport does not
automatically replay an interrupted operation whose result is uncertain. Check
its effect in 1C before asking to run it again.

Toolport restores the MCP connection. Launching Enterprise, opening Vanessa,
and restoring Vanessa's test-client connection and application state remain
the responsibility of the Enterprise/Vanessa session.

## Verification

The isolated regression starts the real gateway as a Codex adapter, starts the
HTTP backend late, makes the application unavailable, restarts its MCP sessions,
and loses a reply after a simulated write. It checks retained tools, a healthy
peer, successful later calls, and exactly one write with no gateway restart:

```sh
cargo test --locked --manifest-path src-tauri/Cargo.toml \
  --no-default-features --features test-support,search-static \
  --test reconnect_failed_servers \
  codex_keeps_access_when_http_backend_starts_late_and_restarts
```

This fixture does not stop or update a real 1C database.
