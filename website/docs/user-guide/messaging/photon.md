---
sidebar_position: 18
---

# Photon iMessage

Connect Hermes to **iMessage** through [Photon][photon], a managed
service that handles the Apple line allocation and abuse-prevention
layer so you don't have to run your own Mac relay.

The free tier uses Photon's shared iMessage line pool — different
recipients may see different sending numbers, but each conversation
stays stable. The paid Business tier gives every user the same
dedicated number; the plugin supports both, and the free tier is the
recommended starting point.

:::info Free to start
Photon's shared-line pool is free. No subscription is required to send
your first iMessage from Hermes — just a phone number we can bind to
your account.
:::

## Architecture

Photon is a **persistent-connection** channel, like Discord or Slack —
**no webhook, no public URL, no signing secret to manage.**

The `spectrum-ts` SDK holds a long-lived **gRPC stream** to Photon for
both directions. Because the SDK is TypeScript-only, Hermes runs it in a
small supervised **Node sidecar** and talks to it over loopback:

- **Inbound** — the sidecar consumes the SDK's `app.messages` gRPC
  stream and forwards each message to the Python adapter over a loopback
  `GET /inbound` (NDJSON). The adapter dedupes and dispatches it to the
  agent, reconnecting automatically if the stream drops.
- **Outbound** — replies are loopback POSTs to the sidecar, which calls
  `space.send(...)` on the SDK. Native replies quote the triggering iMessage;
  streamed model output edits one bubble in place instead of sending fragments.

The Python plugin starts, supervises, and shuts down the sidecar
automatically.

## Prerequisites

- A Photon account — sign up at [app.photon.codes][app]
- **Node.js 20.18.1 or newer** on PATH (`node --version`)
- A phone number that can receive iMessage (used to bind your account)

That's it — there is no public URL or tunnel to set up.

## First-time setup

Either run the unified gateway wizard and pick **Photon iMessage**:

```bash
hermes gateway setup
```

…or run the Photon setup directly (the wizard calls the same flow):

```bash
# Device-code login + project + user + sidecar deps, all in one
hermes photon setup --phone +15551234567
```

The setup, in order:

1. **Device login** (`client_id=photon-cli`) — opens
   `https://app.photon.codes/` for approval and stores the bearer token.
2. **Finds or creates** the `Hermes Agent` project on your account.
3. **Enables Spectrum**, reads the project's Spectrum id, and rotates
   the project secret.
4. **Registers your phone number** as a Spectrum user — skipped if a
   user with that number already exists, so re-running is safe.
5. **Prints your assigned iMessage line** — the number you text to reach
   your agent.
6. **Runs `npm install`** inside the plugin's sidecar directory.

Runtime credentials are written to `~/.hermes/.env`
(`PHOTON_PROJECT_ID` = the Spectrum project id, `PHOTON_PROJECT_SECRET`),
the same place every other channel keeps its token. Management metadata
(device token, dashboard project id) lives in `~/.hermes/auth.json` under
`credential_pool.photon` / `credential_pool.photon_project`.

## Authorizing users

Photon uses the same authorization model as every other Hermes
channel. Choose one approach:

**DM pairing (default).** When an unknown number messages your Photon
line, Hermes replies with a pairing code. Approve it with:

```bash
hermes pairing approve photon <CODE>
```

Use `hermes pairing list` to see pending codes and approved users.

**Pre-authorize specific numbers** (in `~/.hermes/.env`):

```bash
PHOTON_ALLOWED_USERS=+15551234567,+15559876543
```

**Open access** (dev only, in `~/.hermes/.env`):

```bash
PHOTON_ALLOW_ALL_USERS=true
```

When `PHOTON_ALLOWED_USERS` is set, unknown senders are silently
ignored rather than offered a pairing code (the allowlist signals you
deliberately restricted access).

### Require mentions in group chats

By default Hermes responds to every authorized DM and group message.
To make group chats opt-in, enable mention gating (DMs still always
work):

```yaml
gateway:
  platforms:
    photon:
      enabled: true
      require_mention: true
```

With `require_mention: true`, group-chat messages are ignored unless
they match a wake-word pattern. The defaults match `Hermes` and
`@Hermes agent` variants. For a custom agent name, set regex patterns:

```yaml
gateway:
  platforms:
    photon:
      require_mention: true
      mention_patterns:
        - '(?<![\w@])@?amos\b[,:\-]?'
```

Both keys also accept env vars (`PHOTON_REQUIRE_MENTION`,
`PHOTON_MENTION_PATTERNS`). This is the same mention-gating model the
BlueBubbles iMessage channel uses.

## Start the gateway

```bash
hermes gateway start
```

You'll see something like:

```
[photon] connected — sidecar on 127.0.0.1:8789, streaming inbound over gRPC
```

Send an iMessage to your assigned number and Hermes will reply.

### Reactions and read receipts

Photon supports native iMessage tapbacks and read receipts. Read receipts are
enabled by default. Automatic processing tapbacks are opt-in: Hermes adds 👀
while working, then replaces it with 👍 or 👎 when processing finishes.

For a more natural iMessage experience, leave automatic processing tapbacks
disabled. Hermes can still choose an explicit native tapback when it adds
meaning to the conversation; most messages receive no reaction. Explicit
tapbacks are not gated by the automatic `reactions` setting.

```yaml
photon:
  reactions: false
  send_read_receipts: true
```

Environment-variable equivalents are `PHOTON_REACTIONS` and
`PHOTON_SEND_READ_RECEIPTS`. Read receipts are scheduled only after the
gateway's allowlist/pairing authorization check succeeds; unknown senders never
receive one.

### Replies, streaming, edits, and unsend

Hermes uses iMessage's native reply relationship when the gateway supplies a
source message id. Gateway token streaming sends the first visible bubble and
edits it in place as more text arrives. Photon also implements Hermes'
`edit_message` and `delete_message` contracts; deletion maps to iMessage
**Unsend** and is therefore subject to Apple's short unsend window. Spectrum
accepts plain text only for edits, so streamed previews intentionally omit
markdown styling.

### Attachments and voice notes

Inbound images, voice notes, video, and documents are cached as real local media
when Photon can read the bytes. Small files ride the inbound stream directly.
Larger files are fetched by attachment GUID over the authenticated loopback
sidecar, avoiding oversized NDJSON frames. Files above
`PHOTON_MAX_FETCH_ATTACHMENT_BYTES` remain a metadata marker.

Outbound images, voice notes, video, documents, and GIFs use native iMessage
attachments. Captions are sent as a following text bubble because iMessage does
not attach arbitrary captions to media bubbles.

### Effects, contact cards, and mini-app cards

The optional `photon_imessage` plugin tool exposes free/shared-line rich actions
to the agent:

- bubble and screen effects such as `slam`, `invisible_ink`, `confetti`, and
  `fireworks`
- the bot account's native iMessage contact card
- validated customized mini-app cards for developers with their own iMessage
  extension identity
- explicit edit, unsend, recipient-service, and message-state operations

It intentionally does not expose Business-only dedicated-line or group
management actions.

Mini-app identity is operator-controlled; the model cannot supply or spoof it.
Configure it under the Photon block, including an explicit URL-host allowlist:

```yaml
photon:
  mini_app:
    app_name: Hermes
    team_id: ABCDE12345
    extension_bundle_id: codes.example.hermes.MessagesExtension
    app_store_id: 123456789 # optional
    allowed_url_hosts:
      - example.com
```

The v8 SDK does not support live card updates. Optional previews must be safe
local JPEG files no larger than 1.25 MB and include an image title. The generic
URL-only `app()` builder is intentionally not exposed because Spectrum performs
its own metadata fetch; customized cards use operator-approved hosts and supplied
layout data instead.

### Delivery-state semantics

Message status records `accepted`, `edited`, `unsent`, and `failed` operations
performed during the current sidecar lifetime, plus recipient
service (`iMessage`, `SMS`, `RCS`, or `unknown`). `accepted` means Photon
accepted the operation. Spectrum's public cloud SDK does not currently expose
authoritative outbound delivered/read events, so those fields are reported as
unavailable rather than inferred.

## Status & troubleshooting

```bash
hermes photon status
```

Prints saved credentials, sidecar health, your registered number, and the
assigned iMessage line Hermes uses. When a Photon token and dashboard project
are available, `status` refreshes missing number rows from the dashboard
without provisioning new lines.

```
Photon iMessage status
──────────────────────
  device token        : ✓ stored
  project id          : 3c90c3cc-0d44-4b50-...
  project secret      : ✓ stored
  my number           : +15551234567
  assigned number     : +16282679185
  node binary         : /usr/bin/node
  sidecar deps        : ✓ installed
  sidecar listener    : ✓ 127.0.0.1:8789
  telemetry           : off
  subscription        : free (none)
  iMessage line mode  : shared
  documented quotas   : 5,000 messages/day; 50 new conversations/line/day
```

Common issues:

- **`sidecar deps : ✗ run hermes photon install-sidecar`** — Node is
  installed but `spectrum-ts` isn't. Run the suggested command.
- **`device token : ✗ missing`** — run `hermes photon setup` to log in.
- **`No iMessage line assigned yet`** — Spectrum is enabled but no line
  has been provisioned; re-run `hermes photon setup` or check the
  [dashboard][app].
- **Sidecar won't start** — confirm `node --version` is 20.18.1+ and that
  `hermes photon install-sidecar` completed without errors.
- **`target_not_allowed`** — on the free shared-line plan, the recipient must
  send the first iMessage before proactive sends are accepted.
- **`managed_line_target`** — the assigned `TEXTS ON` number is a sending line,
  not the user's recipient number.

## Limits today

- Outbound cloud delivery/read events are not exposed by Spectrum's public SDK.
- Customized mini-app cards require an operator-configured Apple Team ID,
  iMessage extension bundle identity, and URL-host allowlist.
- Contact-card sharing can be rejected by Photon when the account profile is not
  ready; the plugin returns a structured `invalid_request`/`unsupported` error.
- **Photon's free quotas:** 5,000 messages per server per day,
  50 new-conversation initiations per shared line per day. Increases
  available — email `help@photon.codes`.

## Env vars

| Variable                             | Default                 | Notes                                                         |
| ------------------------------------ | ----------------------- | ------------------------------------------------------------- |
| `PHOTON_PROJECT_ID`                  | from `.env`             | Spectrum project id (the SDK's `projectId`); set by setup     |
| `PHOTON_PROJECT_SECRET`              | from `.env`             | Project secret; set by setup                                  |
| `PHOTON_SIDECAR_PORT`                | `8789`                  | Loopback port for the sidecar control + inbound channel       |
| `PHOTON_SIDECAR_AUTOSTART`           | `true`                  | Whether the adapter spawns the sidecar                        |
| `PHOTON_NODE_BIN`                    | `which node`            | Override the Node binary path                                 |
| `PHOTON_HOME_CHANNEL`                | (unset)                 | Default space id for cron / notifications                     |
| `PHOTON_HOME_CHANNEL_NAME`           | (unset)                 | Human label for the home channel                              |
| `PHOTON_ALLOWED_USERS`               | (unset)                 | Comma-separated E.164 allowlist                               |
| `PHOTON_ALLOW_ALL_USERS`             | `false`                 | Dev only — accept any sender                                  |
| `PHOTON_REQUIRE_MENTION`             | `false`                 | Require a wake word before responding in groups               |
| `PHOTON_MENTION_PATTERNS`            | Hermes wake words       | JSON list / comma / newline regex patterns for group mentions |
| `PHOTON_REACTIONS`                   | `false`                 | Automatic 👀 then 👍/👎 processing tapbacks                   |
| `PHOTON_SEND_READ_RECEIPTS`          | `true`                  | Mark authorized inbound iMessages as read                     |
| `PHOTON_MAX_INLINE_ATTACHMENT_BYTES` | `20971520`              | Maximum bytes embedded in one inbound NDJSON event            |
| `PHOTON_MAX_FETCH_ATTACHMENT_BYTES`  | `104857600`             | Maximum attachment size fetched by GUID                       |
| `PHOTON_DASHBOARD_HOST`              | `app.photon.codes`      | Override the dashboard / device-login host                    |
| `PHOTON_SPECTRUM_HOST`               | `spectrum.photon.codes` | Override the Spectrum API host                                |

[photon]: https://photon.codes/
[app]: https://app.photon.codes/
