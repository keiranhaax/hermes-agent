# Photon sidecar

Small Node helper that bridges Hermes Agent to Photon's `spectrum-ts` SDK.
Hermes is Python; Photon messaging is exposed through a persistent TypeScript
SDK connection rather than a public send-message HTTP endpoint.

The sidecar:

- runs `Spectrum({ projectId, projectSecret, providers: [imessage.config()] })`
- consumes the SDK's long-lived `app.messages` stream for inbound messages
- exposes authenticated loopback-only HTTP endpoints for sends, native replies,
  edits, unsend, reactions, read receipts, typing, attachments, effects,
  contact cards, configured mini-app cards, and bounded message status
- forwards inbound messages to the Python adapter as NDJSON over `GET /inbound`
- binds only to `127.0.0.1` and requires `X-Hermes-Sidecar-Token`

## Requirements

- Node.js 20.18.1 or newer
- Photon Spectrum project credentials

## Install

```bash
cd plugins/platforms/photon/sidecar
npm install
```

The Hermes plugin's `hermes photon setup` command installs these dependencies
automatically. `spectrum-ts` is pinned and the postinstall compatibility patch
is a no-op when the upstream package already preserves mixed text and
attachment payloads.

## Run standalone

For debugging:

```bash
PHOTON_PROJECT_ID=... PHOTON_PROJECT_SECRET=... \
PHOTON_SIDECAR_PORT=8789 PHOTON_SIDECAR_TOKEN=$(openssl rand -hex 16) \
node index.mjs
```

In normal use, the Python adapter supervises this process, reconnects its
inbound stream, and shuts it down with the gateway. Users should not run a
second sidecar against the same port.

## Security boundaries

- HTTP request bodies and fetched attachments are size-bounded.
- Large inbound attachments stream to Python rather than entering NDJSON.
- Mini-app identity and allowed URL hosts are operator-configured; the model can
  supply only card content and a validated local JPEG preview.
- Structured errors return curated codes and messages without provider stacks,
  credentials, phone numbers, or raw identifiers.
- The status ledger is process-local and deliberately omits chat and recipient
  identifiers.
