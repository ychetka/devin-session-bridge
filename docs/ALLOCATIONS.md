# Host resource allocations (v0.3.17, directive 426)

The allocation layer is the orchestrator-owned, machine-readable
registry of host-level resources held by projects on this machine.
Projects stay self-sufficient (no per-task ack gates): a project
that needs a host resource declares it once, before deploying, and
the registry guarantees no second project can silently take the
same resource.

The registry is the source of truth for "who owns what on this
host". Enforcement is contractual: deploy scripts MUST call
`bridge reserve` as a pre-flight and hard-fail on a conflict -
sessions-bridge cannot stop a process that never asks, it can only
make collisions visible and arbitrable.

## Commands

```bash
bridge reserve <resource_type> <resource_id> --project <p> [--purpose "..."] [--ttl S]
       [--fwd <spec> [--bind <addr>]]          (tcp_port only)
bridge release <resource_type> <resource_id> --project <p> [--force]
bridge allocations [--type T] [--project P]
bridge ingress sync        # reconcile running forwarders with the registry
bridge ingress status      # desired vs running forwards
```

- `reserve` is atomic: sweep-check-upsert runs inside one
  `BEGIN IMMEDIATE` transaction - the writer lock plus the
  `(resource_type, resource_id)` PRIMARY KEY serialize racers
  end-to-end, so a concurrent reserve sees either a free key or
  the committed incumbent. Exit 0 = granted (`renewed: true` when
  the same project already held the key - a re-reserve touches
  updated_at and keeps the stored purpose/session/expiry when the
  caller passes none; re-stating `--ttl` refreshes the expiry,
  clearing it needs release + re-reserve, so calling reserve() on
  every deploy run is idempotent). Exit 4 (`CONFLICT`) = another
  project holds the
  key; the JSON `data.owner`/`purpose` name the incumbent.
- `release` is owner-bound. A foreign project gets `CONFLICT`
  naming the incumbent. `--force` exists ONLY for orchestrator
  arbitration of a genuinely contested resource - deploy scripts
  must never pass it.
- `allocations` lists live rows (expired TTL rows are swept lazily
  before every registry call - no background watcher).

## Resource types

| `resource_type` | `resource_id` shape | Example |
|---|---|---|
| `tcp_port` | port number | `5678` |
| `systemd_unit_prefix` | unit base name or `{project}-*` prefix | `rootless-docker`, `trade-ai-*` |
| `docker_network_cidr` | CIDR | `10.0.2.0/24` |
| `external_api_account` | account/credential id | `bybit-main`, `telegram-bot-prod` |
| `netns_id` | named netns or logical id | `trade-ai-inner` |
| `other` | free slug | `serial-device-usb0` |

`other` is the escape hatch for a collision domain not listed -
silently skipping a shared resource is worse than a fuzzy type.

## Naming conventions

- **systemd units** are named `{project_id}-<service>` for every
  NEW unit a project creates (e.g. `trade-ai-webhook.service`).
  Projects also reserve the wildcard prefix `systemd_unit_prefix`
  `{project}-*` - an ENFORCED lease, not an advisory note: a
  foreign `P-foo` request conflicts with the `P-*` row (and vice
  versa - requesting `P-*` collides with any foreign `P-*`-covered
  row), checked in the same atomic transaction. Same-project
  requests under an owned wildcard grant normally. Pre-convention
  unit names are registered verbatim (retro-declared rows below).
- **Ports** are reserved individually as `tcp_port/<port>` -
  atomic per-key guarantees. A project MAY additionally declare an
  advisory range as `other/<project>-ports-<start>-<end>`
  (e.g. `trade-ai-ports-5600-5699`): ranges document intent and
  let the orchestrator spot a range squat early, but they are NOT
  atomically enforced - the per-port row is the lease.
- **Port range guidance** (advisory, registered as `other` rows):
  each project documents its working band; collisions inside a
  band are still caught per-port by `tcp_port` rows:

  | project | advisory band | current use |
  |---|---|---|
  | trade-ai | 5600-5699 | 5678 (n8n UI fwd) |
  | trade-ai | 8000-8099 | 8080 (inner :80 fwd) |
  | trade-ai | 8400-8499 | 8443 (TLS fwd, decommissioned ingress) |
  | shared_person | 9200-9299 | 9223 (chrome remote-debug) |

  New projects: pick a 100-port band that `bridge allocations`
  shows free, register the `other/<project>-ports-*` row first,
  then reserve individual ports.

## Deploy-script contract (mandatory)

Every project's deploy/ingress script MUST, before binding a host
resource:

1. `bridge reserve <type> <id> --project <self> [--purpose "..."]`
   for EVERY host resource the deploy will occupy (ports, unit
   names it creates, CIDRs, netns, external accounts).
2. Exit code 0 → proceed. Any non-zero exit → **hard fail**:
   `CONFLICT` means another project holds the resource - print
   `data.owner`/`purpose` and stop. Never retry, never patch the
   registry row, never `--force`.
3. `bridge release <type> <id> --project <self>` on teardown for
   resources that are truly given up (not for restart cycles -
   a held-but-restarting service keeps its row; use `--ttl` only
   for genuinely temporary allocations).
4. Genuinely contested resources (two projects with legitimate
   need) escalate to orchestrator arbitration: a `bridge ask` /
   directive to `buttered-tilapia (ai_controller)`, which decides
   and may use `release --force` + `event` to journal the verdict.
   That is the ONLY escalation path - projects never negotiate
   with each other directly.

## Retro-declared baseline (seeded v0.3.17)

| type / id | project | evidence |
|---|---|---|
| tcp_port/5678 | trade-ai | port_fwd.py host:5678 → inner netns :5678 (n8n UI) |
| tcp_port/8080 | trade-ai | port_fwd.py host:8080 → inner netns :80 |
| tcp_port/8443 | trade-ai | port_fwd.py host:8443 → inner netns :8443 (decommissioned caddy TLS) |
| netns_id/trade-ai-inner | trade-ai | anonymous `unshare -nm` netns holding dockerd (docker-static/supervisor.sh) |
| systemd_unit_prefix/rootless-docker | trade-ai | enabled user unit supervising the stack |
| systemd_unit_prefix/caddy | trade-ai | disabled ingress unit (decommissioned, name held) |
| systemd_unit_prefix/sslh | trade-ai | disabled SNI-mux unit (decommissioned, name held) |
| systemd_unit_prefix/localtunnel | trade-ai | disabled tunnel unit (decommissioned, name held) |
| systemd_unit_prefix/sessions-bridge | sessions-bridge | enabled user unit (this daemon) |
| docker_network_cidr/10.0.2.0/24 | trade-ai | slirp4netns default subnet inside trade-ai-inner |
| tcp_port/9223 | shared_person | chrome `--remote-debugging-port` audit profile |
| systemd_unit_prefix/{devin-telegram,copilot-api,copilot-model-shim,vk-token,voltrack-watch,filter-chain} | (various) | enabled user units, retro-declared |

## Ingress forwards (v0.4.0, task #209)

`tcp_port` reservations can carry a **forward declaration** that the
neutral host ingress layer materializes: `reserve --fwd` stores a
`fwd=<spec>` token inside the row's `purpose`, and the
`host-ingress.timer` (plus explicit `bridge ingress sync` calls from
deploy scripts that want zero-wait publication) reconciles a detached
`session_bridge.ingress.forwarder` process per declared port.

Contract:

- `reserve tcp_port <port> --fwd <spec>` → a listener on
  `<bind>:<port>` appears (at latest within the timer's 30s cadence).
- `release tcp_port <port>` → the forwarder is removed on the next
  reconcile.
- Rows without a `fwd=` token are ownership leases only - no
  forwarder is spawned.

Spec grammar:

| spec | meaning |
|---|---|
| `tcp:<host>:<port>` | relay to host:port in the forwarder's own netns |
| `ns-pid:<pid>:<port>` | setns into `/proc/<pid>/ns/net`, dial 127.0.0.1:<port> |
| `ns-pidfile:<path>:<port>` | same, resolving the pid from an absolute path at reconcile time |

`--bind <addr>` sets the host listen address (default `0.0.0.0`); it
is stored as a `bind=<addr>` token next to `fwd=`.

Reconcile semantics: a forwarder is kept while its row, spec and
resolved target are unchanged; a changed spec, a changed pidfile
target (stack restart) or an unresolvable target kills and respawns
it. The reconciler only kills processes it spawned (cmdline marker +
port must match); malformed rows land in per-row sync errors instead
of blocking other projects' forwards, and a sync with row errors
exits non-zero so exit-code-gated deploys see partial failure.
Forwarder output lands in `<state_dir>/logs/ingress-fwd-<port>.log`;
runtime bookkeeping in `<state_dir>/ingress.json`.

Trust model: `reserve --fwd` is deploy-script code, not untrusted
input - any project may declare a listener (including wildcard binds
or a setns target) for resources it owns; the registry arbitrates
WHO holds a port, not WHAT the forward does. Abuse between projects
is an orchestrator escalation, not a parser concern.

trade-ai's own netns/dockerd stays trade-ai-owned: the project
declares `fwd=ns-pidfile:<docker-static>/inner.pid:<inner>` rows and
the neutral layer does the rest - the hand-rolled `port_fwd.py`
lines in supervisor.sh are superseded by this contract.

## TTL semantics

`--ttl S` sets `expires_at`; expired rows are deleted lazily at
the start of every `reserve`/`release`/`allocations` call inside
the same write transaction, so a freed key is reservable
atomically on the next call. No `--ttl` = held until released -
the default for long-lived service allocations. TTL is for
temporary claims (a canary port, a debug netns), never for a
service meant to stay.

## Out of scope (orchestrator-owned follow-ups)

- Embedding `reserve()` calls into each repo's deploy scripts -
  rolled out per-repo with the root sessions.
