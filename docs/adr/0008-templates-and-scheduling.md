# ADR-0008: Templates and scheduling

- **Status:** Accepted
- **Date:** 2026-11-18
- **Related:** [ADR-0002](0002-postgres-as-outbox-scheduler-and-retry-delay.md), [ADR-0004](0004-data-model-state-machine-and-fencing.md), [ADR-0006](0006-api-contract-and-three-layer-idempotency.md), [ADR-0007](0007-retry-policy-and-circuit-breaker.md), [ADR-0009](0009-observability.md)

## Context

Two bonus features are in scope:

- **Template system:** message templates with variable substitution (R27).
- **Scheduled notifications:** delivery at a future time (R26).

Templates are authored through the API and rendered for up to 1,000 recipients per batch request. They must be safe in three senses:

- **Safe output.** Email bodies are HTML, so variables must be escaped. A missing variable must never produce a message such as *"Your code is `<no value>`"*.
- **Safe execution.** A template must not be able to exhaust CPU or memory, or call anything its author was not meant to reach.
- **Reproducible results.** It must always be possible to tell which template produced a given message.

The scheduling *mechanism* already exists: `scheduled_at` becomes `due_at` and the relay publishes rows when they are due ([ADR-0002](0002-postgres-as-outbox-scheduler-and-retry-delay.md)). What remains is its rules and limits.

## Decision

### Templates

A template has one body. There is no subject template, because a notification has no subject ([ADR-0006](0006-api-contract-and-three-layer-idempotency.md) §3).

#### 1. Engine: Go's standard library, constrained

| Channel | Engine | Why |
|---|---|---|
| SMS and Push | `text/template` | Plain text |
| Email | `html/template` | **Context-aware escaping** of variables prevents HTML injection |

Both engines run with `Option("missingkey=error")`. A missing variable is an error, never `<no value>`.

#### 2. Execution safety: an allow-list of syntax and of function names

When a template is created, its parsed tree (`text/template/parse`) is walked and **only allow-listed constructs are accepted**:

| Allowed | Rejected |
|---|---|
| Text, comments, actions, pipelines, `if` / `else`, string, number and boolean literals, single-level fields (`.first_name`), calls to the two FuncMap functions | `range`, `template`, `block`, `with`, `break`, `continue`, variable declarations (`$x :=`), chained fields (`.a.b`), bare dot (`{{ . }}`), **every builtin function** |

- **Function names are checked, not only node types.** Go's builtin functions (`and`, `call`, `html`, `index`, `slice`, `js`, `len`, `not`, `or`, `print`, `printf`, `println`, `urlquery`, `eq`, `ge`, `gt`, `le`, `lt`, `ne`) are available in every template without being registered, so a node-type check alone would still allow `{{ printf "%999999d" 1 }}`. The walker accepts an identifier only if it is `default` or `truncate`; all 19 builtins are rejected.
- **A template source may define only one template.** `{{ define }}` and `{{ block }}` do not appear as nodes in the tree being walked; they create additional named templates. After parsing, the template set must contain exactly one template, otherwise the source is rejected. This also rules out recursion.
- An allow-list was chosen over a deny-list because a deny-list fails open as soon as the language gains a new construct.
- **Output limits do not prevent CPU exhaustion:** `{{ range 1000000000 }}{{ end }}` produces no output at all. Rejecting `range` removes the whole class of problem.
- **Size limits:** a template body is at most **64 KB**. As defence in depth, rendering writes into a `limitedWriter` that fails above 512 KB.

#### 3. Functions

The FuncMap is restricted to **`default` and `truncate`**.

Go pipelines pass the piped value as the **last** argument. The signatures are therefore `default(fallback, value string)` and `truncate(n int, s string)`, so that `{{ .name | default "Customer" }}` and `{{ .text | truncate 50 }}` behave as expected. A test covers each function in pipeline form.

#### 4. Declared variables

- Every template version declares its variables in two lists: `variables` (required) and `optional_variables`.
- At creation:
  - every field referenced in the template must be declared in one of the lists;
  - a **declared variable that the template does not use** is rejected, so the declaration cannot drift from the body.
- At render time:
  - a **missing required** variable returns `422`;
  - a **missing optional** variable is rendered as the empty string, which is what makes `default` useful: `{{ .first_name | default "Customer" }}`;
  - an **undeclared extra** variable returns `422`, so a typo such as `fist_name` is reported instead of silently ignored;
  - at most 50 variables, each value at most 1,024 characters.
- **Value types.** A value may be a JSON string, number or boolean; it is converted to a string. Numbers are decoded as `json.Number`, so `482913` stays `482913` and a long numeric code is not rewritten in exponent form. Objects and arrays return `422`.

#### 5. Render at creation, then validate the result

Notifications are rendered **when they are created**, and the rendered `content` is stored ([ADR-0004](0004-data-model-state-machine-and-fencing.md)):

- Validation errors surface immediately as `422`, not hours later in a worker.
- Changing a template never alters messages that are already accepted or queued.

**Validation runs on the rendered output** ([ADR-0006](0006-api-contract-and-three-layer-idempotency.md) §5): channel limits, including the SMS segment count, are applied to the rendered text.

A template's channel is fixed. Requesting it for a different channel returns `422`.

#### 6. Immutable versions

```
templates          (name PK, channel, is_active, latest_version, created_at, updated_at)
template_versions  (template_name, version, body_tpl, variables text[], optional_variables text[],
                    created_at, PRIMARY KEY (template_name, version))
```

- **Versions are never modified.** An update creates a new version.
- **Creating a template whose name already exists returns `409`.** New content for an existing name goes through the versions endpoint.
- **Creating a version locks the parent row:** `SELECT ... FROM templates WHERE name = $1 FOR UPDATE`, then `latest_version + 1`. This avoids the race in which two concurrent `MAX(version) + 1` reads produce the same number and the second insert fails with a unique violation.
- A request may pin a version (`{"name": "otp", "version": 3}`); otherwise the latest version is used. Notifications record `template_name`, `template_version` and `template_vars`, so the provenance of every message is exact.
- Template names match `^[a-z0-9][a-z0-9_-]{0,63}$`.

#### 7. Deactivation, not deletion

- Deactivation sets `is_active = false` on the parent row.
- **No new notification can use an inactive template**, even with an explicitly pinned version (`422 template_inactive`).
- Existing notifications are unaffected, because their content was rendered at creation.
- Templates are never deleted, so historical records always resolve. `/activate` reverses a deactivation.

#### 8. Caching

- Parsed templates are cached **for the duration of one request**, in a map keyed by `(name, version)`. A batch of 1,000 notifications parses each template at most once.
- There is no process-wide cache. Nothing needs to be invalidated or bounded, and a deactivation takes effect on the next request.

#### 9. Endpoints

| Method | Path | Purpose |
|---|---|---|
| `POST` | `/v1/templates` | Create a template (version 1); `409` if the name exists |
| `POST` | `/v1/templates/{name}/versions` | Add a version |
| `GET` | `/v1/templates/{name}` | Template with its latest version |
| `GET` | `/v1/templates/{name}/versions/{version}` | A specific version |
| `POST` | `/v1/templates/{name}/preview` | **Dry run:** render with the given variables and return `{content, encoding, segments}`. Nothing is stored. |
| `POST` | `/v1/templates/{name}/deactivate` | Deactivate |
| `POST` | `/v1/templates/{name}/activate` | Reactivate |

### Scheduling

| Topic | Rule |
|---|---|
| Mechanism | `due_at = scheduled_at`; the relay publishes the row when it is due ([ADR-0002](0002-postgres-as-outbox-scheduler-and-retry-delay.md)) |
| Format | RFC 3339 with an explicit offset; stored as UTC (`timestamptz`) |
| Past timestamps | Up to 30 seconds in the past means "now" (clock-skew tolerance between client, api and database). Such a value is **stored as `NULL`**: the notification is not a scheduled one, so its latency is measured from creation and its trace follows the non-scheduled rule ([ADR-0009](0009-observability.md)). Older timestamps return `422`. |
| Horizon | At most 30 days ahead |
| Expiry | Defaults to `scheduled_at + TTL` from the priority profile ([ADR-0007](0007-retry-policy-and-circuit-breaker.md) §3). An explicit `expires_at` must be at least `COALESCE(scheduled_at, now()) + 30s`; otherwise a notification scheduled for five days ahead could expire after one day without ever being attempted. |
| Precision | About 100 ms: the relay's tick interval ([ADR-0002](0002-postgres-as-outbox-scheduler-and-retry-delay.md)) |
| Many notifications scheduled for the same instant (e.g. 1 M at 10:00) | **No special mechanism.** It is the same situation as a flash sale and is absorbed by the lane HWMs and strict priority ([ADR-0003](0003-bounded-relay-buffer-and-sweepers.md)). |
| Cancelling | A scheduled notification is `pending`, so it can be cancelled up to the moment a worker claims it (transition 9, [ADR-0004](0004-data-model-state-machine-and-fencing.md)) |
| Rescheduling | Not supported: cancel and create again (YAGNI) |
| Latency measurement | End-to-end latency is measured from `COALESCE(scheduled_at, created_at)`, so scheduled notifications do not appear hours late ([ADR-0009](0009-observability.md)) |

Per-recipient time zones, quiet hours (for example no campaigns between 22:00 and 09:00) and recurring schedules are [future work](../future-work.md).

## Alternatives considered

| Alternative | Why it was rejected |
|---|---|
| **An allow-list of node types only** | Builtin functions are reachable without a FuncMap entry, and `define` creates templates the walker never visits. Both gaps are closed by checking function names and the number of templates in the set (section 2). |
| **`upper` and `lower` in the FuncMap** | Case mapping depends on the language: in Turkish, `i` and `ı` do not map as Go's default functions assume. Rather than ship functions that are wrong for some recipients, they are left out. |
| **A process-wide LRU cache of parsed templates** | Needs a size bound and reasoning about staleness for a saving that only matters within a batch. A per-request map gives the same saving. |
| **A `subject_tpl` per template version** | A notification has no subject ([ADR-0006](0006-api-contract-and-three-layer-idempotency.md)). |
| **Every declared variable is required** | `default` would almost never apply, because a missing variable is already an error. `optional_variables` makes the intent explicit. |
| **A minimal `{{var}}` substitution engine of our own** | Simple and predictable, but email escaping would have to be hand-written, which is where such engines usually go wrong. The standard library with an allow-list gives the same predictability. |
| **Third-party engines** (Mustache, Handlebars or Jinja-like) | An extra dependency and another escaping model to audit; there is no capability we need beyond the standard library. |
| **A deny-list of dangerous syntax** | Fails open when the language gains a construct. |
| **Only an output size limit** | Does not stop loops that produce no output. |
| **Mutable templates, edited in place** | Loses the record of what was actually sent. |
| **Render at send time** | Validation errors would appear late and asynchronously, and template edits would change messages that are already queued. |
| **Inferring variables from the parse tree only** | Cannot distinguish an intentionally optional variable from a typo; declared variables make the contract explicit. |
| **`MAX(version) + 1` without a parent lock** | Race between concurrent version creations. |
| **Scheduling with broker delays or a separate scheduler** | See [ADR-0001](0001-rabbitmq-as-dispatch-broker.md) and [ADR-0002](0002-postgres-as-outbox-scheduler-and-retry-delay.md). |

## Consequences

**Positive**

- Template output is safe by construction: escaping where it matters, no silent missing values, no reachable builtin functions and no constructs that can exhaust resources.
- Every message records exactly which template version produced it, and that version can never change.
- Batch rendering is cheap without any shared cache.
- Scheduling adds no new moving parts; it reuses the relay and inherits its backpressure.

**Negative / costs**

- The allow-list makes templates deliberately less expressive: no loops, no nested data, no comparisons and no case conversion. This is acceptable for notification copy, and can be relaxed later behind the same validation step.
- Template authors must declare variables, which adds a step but turns typos into errors.
- Rendering at creation stores the full content for every notification, which increases storage.
- Each request parses the templates it uses again.

## Review history

Changes made after the design review, recorded in the [design review log](../ai/design-review-log.md):

- The allow-list now checks function names and rejects sources that define more than one template, because builtin functions and `define` bypassed a check of node types. (D40)
- `optional_variables`, accepted value types, the 64 KB body limit and three previously undefined cases (unused declared variable, duplicate name, past `scheduled_at` within tolerance) were specified. (D40)
- `upper` and `lower` were removed, and the process-wide cache became a per-request map. (D40)
- `subject_tpl` and all subject rules were removed together with the `subject` field. (D22)
- Scheduling precision is about 100 ms, following the relay's fixed ticker. (D3)
