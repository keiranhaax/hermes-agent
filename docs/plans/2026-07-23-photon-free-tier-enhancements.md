# Photon Free-Tier Enhancements Implementation Plan

> **For Hermes:** Use subagent-driven-development skill to implement this plan task-by-task.

**Goal:** Complete the Photon iMessage integration’s free/shared-line feature set without cross-platform identity merging or Photon Business/dedicated-line operations.

**Architecture:** Keep Photon-specific capability at the plugin edge. Extend the supervised Node sidecar with authenticated loopback endpoints, implement standard `BasePlatformAdapter` methods for reply/edit/delete streaming integration, and expose optional rich iMessage operations through a Photon-scoped plugin tool rather than expanding the core `send_message` schema. Preserve the existing uncommitted reactions/read-receipts work.

**Tech Stack:** Python 3.11, asyncio/httpx, Node.js 22, `spectrum-ts`, pytest, Hermes plugin and gateway APIs.

---

## Constraints and acceptance criteria

- Preserve current work in `plugins/platforms/photon/adapter.py`, `plugins/platforms/photon/sidecar/index.mjs`, `website/docs/user-guide/messaging/photon.md`, and `tests/plugins/platforms/photon/test_read_receipts.py`.
- Do not link Telegram and iMessage identities or sessions.
- Do not add group creation/management, dedicated-line routing, group metadata events, or auto-scale operations.
- Do not expose Spectrum's generic `app()` card builder: it performs its own
  metadata fetch, which cannot be safely pinned across DNS changes and redirects.
- Never expose credentials, raw stack traces, or unrestricted local file paths.
- Every feature needs focused tests and a final Photon regression run.
- Unsupported upstream facts must be reported honestly. The current Spectrum public API does not provide authoritative remote delivered/read events for outbound cloud messages; expose accepted/failed/edited/unsent state plus recipient service, and label delivered/read observation as unavailable rather than fabricating it.

### Task 1: Native replies and editable streaming

**Objective:** Make ordinary Hermes replies quote the triggering iMessage and let `GatewayStreamConsumer` edit an in-progress response in place.

**Files:**

- Modify: `plugins/platforms/photon/sidecar/index.mjs`
- Modify: `plugins/platforms/photon/adapter.py`
- Create: `tests/plugins/platforms/photon/test_reply_edit.py`

**Steps:**

1. Write failing adapter tests proving `send(..., reply_to=id)` forwards `replyTo`, `edit_message()` calls `/edit`, and `delete_message()` calls `/unsend`.
2. Write sidecar contract tests or source assertions for `/send` reply handling, `/edit`, `/unsend`, and outbound-message tracking.
3. Import Spectrum reply/edit/unsend helpers or use hydrated Message sugar methods.
4. Remember live outbound Message handles. Do not fake edit/unsend rehydration
   after restart: Spectrum rehydrates those IDs as inbound wrappers, so return a
   stable `not_found` result while still allowing reply targets to rehydrate.
5. Pass `reply_to` through text and media sends where Spectrum supports it.
6. Implement adapter `edit_message` and `delete_message` so the existing gateway stream consumer can progressively edit responses.
7. Run focused tests and the Photon test directory.

### Task 2: Structured errors and delivery-state ledger

**Objective:** Replace generic sidecar errors with sanitized machine-readable errors and maintain honest send-state information.

**Files:**

- Modify: `plugins/platforms/photon/sidecar/index.mjs`
- Modify: `plugins/platforms/photon/adapter.py`
- Create: `tests/plugins/platforms/photon/test_errors_status.py`

**Steps:**

1. Write tests for normalized codes: `target_not_allowed`, `managed_line_target`, `quota_exceeded`, `not_found`, `unsupported`, `auth_failed`, `upstream_unavailable`, and `internal_error`.
2. Sanitize SDK errors into `{ok:false,error:{code,message,retryable}}` without stack traces or secrets.
3. Teach `_sidecar_call` to preserve safe code/message details in `SendResult.error`.
4. Track bounded message states for accepted, failed, edited, and unsent operations.
5. Add recipient-service lookup through `imessage(app).user(...)` and expose iMessage/SMS/RCS/unknown.
6. Explicitly report outbound delivered/read observation as unavailable when the public SDK cannot provide it.

### Task 3: Large attachment retrieval

**Objective:** Fetch oversized inbound attachments by GUID without embedding their bytes into NDJSON.

**Files:**

- Modify: `plugins/platforms/photon/sidecar/index.mjs`
- Modify: `plugins/platforms/photon/adapter.py`
- Modify: `tests/plugins/platforms/photon/test_inbound.py`

**Steps:**

1. Write tests for metadata-only inbound attachments triggering a sidecar fetch.
2. Add an authenticated `/attachment` endpoint using `imessage(app).getAttachment(guid, phone)` with ID validation and a bounded maximum size.
3. Return attachment bytes with safe MIME/name headers, not JSON/base64.
4. Stream the response to Hermes’ media cache using an atomic temporary file and path validation.
5. Keep a metadata-only marker when unavailable, too large, or retrieval fails.

### Task 4: Free-tier rich iMessage actions

**Objective:** Add effects, contact-card sharing, and customized mini-app cards without Business-only operations or Spectrum's metadata-fetching generic app cards.

**Files:**

- Create: `plugins/platforms/photon/tools.py`
- Modify: `plugins/platforms/photon/sidecar/index.mjs`
- Modify: `plugins/platforms/photon/adapter.py`
- Modify: `plugins/platforms/photon/adapter.py` plugin registration
- Create: `tests/plugins/platforms/photon/test_rich_actions.py`

**Steps:**

1. Define a Photon-scoped plugin tool with actions `effect`, `share_contact`, `custom_app_card`, `edit`, `unsend`, `message_status`, and `recipient_status`.
2. Resolve only the live Photon adapter; fail clearly in cron/standalone contexts requiring live message state.
3. Validate effect names against a fixed allowlist.
4. Keep Team ID, bundle ID, App Store ID, and URL-host allowlist operator-configured; validate model-supplied URLs, text lengths, and optional image paths before sidecar calls.
5. Add sidecar endpoints using `effect`, `nativeContactCard`, and `customizedMiniApp`.
6. Record returned message IDs and states where the operation creates a message.
7. Do not implement any group/dedicated-line action.

### Task 5: Operational polish

**Objective:** Improve CLI discoverability, startup behavior, chat labels, status, and dependency hygiene.

**Files:**

- Modify: `hermes_cli/main.py`
- Modify: `plugins/platforms/photon/adapter.py`
- Modify: `plugins/platforms/photon/cli.py`
- Modify: `plugins/platforms/photon/sidecar/package.json`
- Modify: `plugins/platforms/photon/sidecar/package-lock.json`
- Modify/add focused tests under `tests/hermes_cli/` and `tests/plugins/platforms/photon/`

**Steps:**

1. Add a regression test proving `hermes photon --help` resolves only the deferred Photon platform plugin instead of importing every platform.
2. During unknown-subcommand discovery, resolve a matching deferred platform before reading registered CLI commands.
3. Default Photon `gateway_restart_notification` to false so shared-line startup does not proactively message recipients.
4. Use the sender address for DM chat names and Spectrum display names for chats when available, rather than raw GUIDs.
5. Extend `hermes photon status` with sidecar port/listener health, documented free-tier quotas, assigned-line mode, and safe operational hints.
6. Upgrade the exact Spectrum pin deliberately, regenerate the lockfile, preserve/update the compatibility patch, run `npm audit`, and do not use `npm audit fix --force`.

### Task 6: Documentation and verification

**Objective:** Document the final behavior and prove integration quality.

**Files:**

- Modify: `website/docs/user-guide/messaging/photon.md`
- Modify: `plugins/platforms/photon/plugin.yaml` if new settings/tool metadata require it

**Steps:**

1. Document native replies, streaming edits, unsend window, large-attachment fallback, structured errors, rich actions, and status limitations.
2. State clearly that Business-only group/dedicated-line features are excluded.
3. Run focused tests for every new file.
4. Run `scripts/run_tests.sh tests/plugins/platforms/photon -q`.
5. Run relevant gateway/CLI/plugin tests.
6. Run syntax checks and `npm audit --omit=dev`.
7. Perform spec-compliance review, code-quality review, and final integration review.
8. Restart the gateway, verify Photon reconnects, then live-test reply, streaming/edit, unsend, effect/contact/card actions only after explicit external-send approval where needed.
