// Hermes Agent — Photon Spectrum sidecar
//
// Spawned by `plugins/platforms/photon/adapter.py` to bridge BOTH directions
// of messaging to Photon's Spectrum platform via the `spectrum-ts` SDK (the
// SDK is TypeScript-only, so a Node sidecar is unavoidable — there is no
// Python SDK and no public HTTP message API).
//
// Inbound  (gRPC -> Hermes): the SDK's `app.messages` async iterator is a
//   long-lived gRPC stream. We serialize each `[space, message]` to a
//   normalized JSON event and stream it to the Python adapter over a
//   loopback `GET /inbound` (NDJSON). We pause pulling from the stream while
//   no consumer is attached so a backlog isn't pulled-and-lost before the
//   gateway connects.
// Outbound (Hermes -> gRPC): `/send` drives `space.send(...)`; `/typing`
//   sends the documented `typing("start" | "stop")` content builder.
//
// Protocol (all requests require `X-Hermes-Sidecar-Token: ${TOKEN}`):
//   - GET  /inbound    -> 200 NDJSON stream; one JSON event per line, blank
//                         lines are heartbeats. One consumer at a time.
//   - POST /healthz     -> {"ok": true}
//   - POST /send        -> {"ok": true, "messageId": "..."}
//       body: {"spaceId": "...", "text": "...", "replyTo": "..." | null,
//              "format": "text" | "markdown" (default "text")}
//   - POST /edit        -> edit an outbound message in place
//   - POST /unsend      -> retract a recent outbound message
//   - POST /attachment  -> raw attachment bytes fetched by GUID
//   - POST /effect      -> send text/markdown with an iMessage effect
//   - POST /contact-card -> share the bot account's native contact card
//   - POST /custom-app-card -> operator-identified iMessage mini-app card
//   - POST /message-status | /recipient-status -> bounded operational status
//   - POST /send-attachment -> {"ok": true, "messageId": "..."}
//       body: {"spaceId": "...", "path": "...", "name": "..." | null,
//              "mimeType": "..." | null, "caption": "..." | null,
//              "kind": "attachment" | "voice"}
//   - POST /react       -> {"ok": true, "reactionId": "..." | null}
//       body: {"spaceId": "...", "messageId": "<target msg id>",
//              "emoji": "👀"}
//   - POST /unreact     -> {"ok": true} | 400 soft failure
//       body: {"spaceId": "...", "messageId": "<target msg id>",
//              "reactionId": "..." | null (restart-recovery fallback)}
//   - POST /read        -> {"ok": true}
//       body: {"spaceId": "...", "messageId": "<inbound msg id>"}
//   - POST /typing      -> {"ok": true}
//       body: {"spaceId": "...", "state": "start" | "stop"}
//   - POST /shutdown    -> {"ok": true}; then process exits
//
// On SIGINT/SIGTERM the sidecar calls `app.stop()` (3s graceful) before
// exiting. Logs go to stderr; Python supervises restart.
//
// Requires spectrum-ts 8.x — pinned exactly in package.json because the SDK
// ships breaking majors; see README "Upgrading spectrum-ts".
//
// Env vars (required):
//   PHOTON_PROJECT_ID      (== the project's spectrumProjectId)
//   PHOTON_PROJECT_SECRET
//   PHOTON_SIDECAR_PORT
//   PHOTON_SIDECAR_TOKEN
// Optional:
//   PHOTON_SIDECAR_BIND    (default 127.0.0.1)
//   PHOTON_SIDECAR_WATCH_STDIN  "1" = exit when stdin hits EOF (set by the
//                          adapter, which holds our stdin pipe — parent-death
//                          detection so a dead gateway can't orphan us)
//   PHOTON_TELEMETRY       enable Spectrum SDK telemetry ("true"/"1"/"on"/"yes";
//                          default off — toggle with `hermes photon telemetry`)

import http from "node:http";
import crypto from "node:crypto";
import net from "node:net";
import { once } from "node:events";
import { patchSpectrumTs } from "./patch-spectrum-mixed-attachments.mjs";

const projectId = process.env.PHOTON_PROJECT_ID;
const projectSecret = process.env.PHOTON_PROJECT_SECRET;
const port = parseInt(process.env.PHOTON_SIDECAR_PORT || "8789", 10);
const bind = process.env.PHOTON_SIDECAR_BIND || "127.0.0.1";
const sharedToken = process.env.PHOTON_SIDECAR_TOKEN;
const telemetry = /^(1|true|yes|on)$/i.test(
  (process.env.PHOTON_TELEMETRY || "").trim()
);

// Inbound binary content is read into memory and base64-inlined on the NDJSON
// event so the Python adapter can cache the real bytes (and the agent can see
// images / transcribe voice). Cap the size we inline — above it we forward
// metadata only and the adapter surfaces a text marker, so one large clip can't
// balloon a single NDJSON line. Override via PHOTON_MAX_INLINE_ATTACHMENT_BYTES.
const MAX_INLINE_ATTACHMENT_BYTES =
  Number(process.env.PHOTON_MAX_INLINE_ATTACHMENT_BYTES) || 20 * 1024 * 1024;
const MAX_FETCH_ATTACHMENT_BYTES =
  Number(process.env.PHOTON_MAX_FETCH_ATTACHMENT_BYTES) || 100 * 1024 * 1024;
const DM_CHAT_GUID_RE = /^any;-;(\+\d{6,})$/;
const E164_RE = /^\+\d{6,}$/;
const MAX_KNOWN_SPACES = 2048;
const MAX_KNOWN_MESSAGES = 1024;
const MAX_REACTION_HANDLES = 512;
const MAX_MESSAGE_STATES = 2048;
const IMESSAGE_EFFECTS = Object.freeze({
  slam: "com.apple.MobileSMS.expressivesend.impact",
  loud: "com.apple.MobileSMS.expressivesend.loud",
  gentle: "com.apple.MobileSMS.expressivesend.gentle",
  invisible_ink: "com.apple.MobileSMS.expressivesend.invisibleink",
  confetti: "com.apple.messages.effect.CKConfettiEffect",
  fireworks: "com.apple.messages.effect.CKFireworksEffect",
  balloons: "com.apple.messages.effect.CKBalloonEffect",
  heart: "com.apple.messages.effect.CKHeartEffect",
  lasers: "com.apple.messages.effect.CKLasersEffect",
  celebration: "com.apple.messages.effect.CKHappyBirthdayEffect",
  sparkles: "com.apple.messages.effect.CKSparklesEffect",
  spotlight: "com.apple.messages.effect.CKSpotlightEffect",
  echo: "com.apple.messages.effect.CKEchoEffect",
});
const STREAM_DEGRADED_RESTART_MS =
  Number(process.env.PHOTON_STREAM_DEGRADED_RESTART_MS) || 90 * 1000;
const STREAM_INTERRUPTED_DEGRADE_COUNT =
  Number(process.env.PHOTON_STREAM_INTERRUPTED_DEGRADE_COUNT) || 3;

const streamHealth = {
  state: "starting",
  degradedSince: null,
  lastHealthyAt: null,
  lastIssueAt: null,
  lastIssue: null,
  issueCount: 0,
};
let streamRestartTimer = null;

function streamHealthSnapshot() {
  const now = Date.now();
  const degradedForMs =
    streamHealth.degradedSince === null ? 0 : now - streamHealth.degradedSince;
  return {
    ok: streamHealth.state !== "degraded",
    state: streamHealth.state,
    degradedForMs,
    restartAfterMs: STREAM_DEGRADED_RESTART_MS,
    lastHealthyAt: streamHealth.lastHealthyAt,
    lastIssueAt: streamHealth.lastIssueAt,
    lastIssue: streamHealth.lastIssue,
    issueCount: streamHealth.issueCount,
  };
}

function markStreamHealthy() {
  streamHealth.state = "healthy";
  streamHealth.degradedSince = null;
  streamHealth.lastHealthyAt = new Date().toISOString();
  streamHealth.issueCount = 0;
  if (streamRestartTimer) {
    clearTimeout(streamRestartTimer);
    streamRestartTimer = null;
  }
}

function scheduleStreamRestart() {
  if (STREAM_DEGRADED_RESTART_MS <= 0 || streamRestartTimer) return;
  streamRestartTimer = setTimeout(() => {
    streamRestartTimer = null;
    if (
      streamHealth.state !== "degraded" ||
      streamHealth.degradedSince === null
    ) {
      return;
    }
    const degradedForMs = Date.now() - streamHealth.degradedSince;
    if (degradedForMs < STREAM_DEGRADED_RESTART_MS) {
      scheduleStreamRestart();
      return;
    }
    console.error(
      `photon-sidecar: upstream stream degraded for ${degradedForMs}ms; ` +
        "exiting so Hermes can restart the Photon adapter"
    );
    process.exit(75);
  }, STREAM_DEGRADED_RESTART_MS + 1000);
  streamRestartTimer.unref();
}

function markStreamDegraded(reason) {
  const now = Date.now();
  if (streamHealth.state !== "degraded") {
    streamHealth.degradedSince = now;
  }
  streamHealth.state = "degraded";
  streamHealth.lastIssueAt = new Date(now).toISOString();
  streamHealth.lastIssue = reason;
  streamHealth.issueCount += 1;
  scheduleStreamRestart();
}

function markStreamRecovering(reason) {
  if (streamHealth.state !== "recovering") {
    streamHealth.issueCount = 0;
  }
  streamHealth.state = "recovering";
  streamHealth.lastIssueAt = new Date().toISOString();
  streamHealth.lastIssue = reason;
  streamHealth.issueCount += 1;
  if (streamHealth.issueCount >= STREAM_INTERRUPTED_DEGRADE_COUNT) {
    markStreamDegraded(reason);
  }
}

function classifyStreamLog(text) {
  if (!text.includes("[spectrum.stream]")) return;
  const reason = text.split("\n", 1)[0];
  if (text.includes("persistently failing")) {
    markStreamDegraded(reason);
  } else if (text.includes("stream interrupted")) {
    markStreamRecovering(reason);
  }
}

// spectrum-ts routes its stream telemetry through @photon-ai/otel's
// createLogger, which sends severity >= ERROR to console.error and
// everything else (WARN/INFO) to console.log. The two lines we key off
// land on *different* channels: `log.error("stream persistently failing")`
// -> console.error, but `log.warn("stream interrupted; reconnecting")`
// -> console.log. Patch both so the recovering/degraded counters see the
// interrupt bursts, not just the terminal "persistently failing" line.
const originalConsoleError = console.error.bind(console);
console.error = (...args) => {
  const text = args
    .map((arg) => (arg && arg.stack ? arg.stack : String(arg)))
    .join(" ");
  classifyStreamLog(text);
  originalConsoleError(...args);
};

const originalConsoleLog = console.log.bind(console);
console.log = (...args) => {
  const text = args
    .map((arg) => (arg && arg.stack ? arg.stack : String(arg)))
    .join(" ");
  classifyStreamLog(text);
  originalConsoleLog(...args);
};

if (!projectId || !projectSecret || !sharedToken) {
  console.error(
    "photon-sidecar: PHOTON_PROJECT_ID, PHOTON_PROJECT_SECRET and " +
      "PHOTON_SIDECAR_TOKEN must all be set."
  );
  process.exit(2);
}

// Lazy-load spectrum-ts so a missing install fails with a clear message
// instead of a cryptic module-resolution error during import. Apply Hermes'
// pinned-sdk compatibility patch first so existing installs self-heal at
// runtime, not only during npm postinstall.
try {
  const patchResult = patchSpectrumTs();
  if (patchResult.patched) {
    console.error(
      `photon-sidecar: spectrum mixed attachment patch applied: ${patchResult.file}`
    );
  }
} catch (e) {
  console.error(
    "photon-sidecar: spectrum mixed attachment patch failed. " +
      "Run `npm install` inside plugins/platforms/photon/sidecar/ or " +
      "upgrade the Photon sidecar patch for the pinned spectrum-ts version. " +
      "Original error: " +
      (e && e.stack ? e.stack : String(e))
  );
  process.exit(3);
}
let Spectrum,
  imessage,
  imessageEffect,
  nativeContactCard,
  customizedMiniApp,
  attachment,
  voice,
  spectrumReply,
  spectrumEdit,
  spectrumUnsend,
  spectrumText,
  spectrumMarkdown,
  spectrumTyping;
try {
  ({
    Spectrum,
    attachment,
    voice,
    reply: spectrumReply,
    edit: spectrumEdit,
    unsend: spectrumUnsend,
    text: spectrumText,
    markdown: spectrumMarkdown,
    typing: spectrumTyping,
  } = await import("spectrum-ts"));
  ({
    imessage,
    effect: imessageEffect,
    nativeContactCard,
    customizedMiniApp,
  } = await import("spectrum-ts/providers/imessage"));
} catch (e) {
  console.error(
    "photon-sidecar: spectrum-ts is not installed. Run `npm install` " +
      "inside plugins/platforms/photon/sidecar/. Original error: " +
      (e && e.stack ? e.stack : String(e))
  );
  process.exit(3);
}

const app = await Spectrum({
  projectId,
  projectSecret,
  providers: [imessage.config()],
  options: { flattenGroups: true },
  telemetry,
});

// ---------------------------------------------------------------------------
// Inbound: forward `app.messages` (gRPC stream) to the Python consumer.

// At most one Python consumer is attached at a time (the gateway adapter).
let consumerRes = null;
let consumerWaiters = [];
const knownSpaces = new Map();
// Inbound Message objects by id, so /react can usually skip a
// `space.getMessage` round trip when tapping back on a recent message.
const knownMessages = new Map();
// Bounded operational ledger for sends made during this sidecar lifetime.
// "accepted" means Photon accepted the operation; Spectrum's public cloud API
// does not currently expose authoritative outbound delivered/read events.
const messageStates = new Map();
// One reaction handle per reacted-to message (key `${spaceId}\0${messageId}`,
// value {emoji, handle}) — mirrors iMessage's one-tapback-per-sender
// semantics; a new /react on the same target overwrites the slot. The handle
// is the outbound reaction Message returned by `target.react()`, kept so
// /unreact can `unsend()` it later.
const reactionHandles = new Map();

function lruSet(map, key, value, cap) {
  if (map.has(key)) map.delete(key);
  map.set(key, value);
  if (map.size > cap) {
    const oldest = map.keys().next().value;
    if (oldest !== undefined) map.delete(oldest);
  }
}

function rememberKnownSpace(id, space) {
  if (!id || typeof id !== "string" || !space) return;
  lruSet(knownSpaces, id, space, MAX_KNOWN_SPACES);
}

function rememberKnownMessage(message) {
  const id = message?.id;
  if (!id || typeof id !== "string") return;
  lruSet(knownMessages, id, message, MAX_KNOWN_MESSAGES);
}

function rememberMessageState(messageId, state, extra = {}) {
  if (!messageId || typeof messageId !== "string") return;
  lruSet(
    messageStates,
    messageId,
    {
      messageId,
      state,
      updatedAt: new Date().toISOString(),
      delivered: null,
      read: null,
      deliveryObservation: "unavailable",
      ...extra,
    },
    MAX_MESSAGE_STATES
  );
}

function phoneTargetFromSpaceId(spaceId) {
  if (typeof spaceId !== "string") return null;
  if (E164_RE.test(spaceId)) return spaceId;
  const dmGuid = spaceId.match(DM_CHAT_GUID_RE);
  return dmGuid ? dmGuid[1] : null;
}

function rememberInboundSpace(space, message) {
  const msgSpace = message?.space || {};
  const ids = [space?.id, msgSpace.id];
  for (const id of ids) {
    rememberKnownSpace(id, space);
    const phone = phoneTargetFromSpaceId(id);
    if (phone) rememberKnownSpace(phone, space);
  }
}

function waitForConsumer() {
  if (consumerRes) return Promise.resolve();
  return new Promise((resolve) => consumerWaiters.push(resolve));
}

function setConsumer(res) {
  consumerRes = res;
  const waiters = consumerWaiters;
  consumerWaiters = [];
  for (const resolve of waiters) resolve();
}

function clearConsumer(res) {
  if (consumerRes === res) consumerRes = null;
}

// Write one NDJSON line to the active consumer. Blocks until a consumer is
// connected; if the write fails (consumer vanished mid-flight) we wait for a
// new consumer and retry, so a message is never silently dropped here.
async function deliver(line) {
  for (;;) {
    await waitForConsumer();
    const res = consumerRes;
    if (!res) continue;
    try {
      const flushed = res.write(line + "\n");
      if (!flushed) await once(res, "drain");
      return;
    } catch {
      clearConsumer(res);
    }
  }
}

async function normalizeBinaryContent(content) {
  const meta = {
    type: content.type,
    id: content.id ?? null,
    name: content.name ?? null,
    mimeType: content.mimeType ?? null,
    size: typeof content.size === "number" ? content.size : null,
  };
  if (content.type === "voice" && typeof content.duration === "number") {
    meta.duration = content.duration;
  }

  // Inline only bounded streams. Larger or unreadable content stays metadata-only
  // and the Python adapter retrieves it later by attachment GUID.
  const label = `${content.type} ${meta.name ?? meta.id ?? "(unnamed)"}`;
  if (meta.size !== null && meta.size > MAX_INLINE_ATTACHMENT_BYTES) {
    console.error(
      `photon-sidecar: ${label} (${meta.size} bytes) ` +
        `exceeds inline cap ${MAX_INLINE_ATTACHMENT_BYTES}; forwarding metadata only`
    );
    return meta;
  }
  if (typeof content.stream === "function") {
    try {
      const source = await content.stream();
      const chunks = [];
      let total = 0;
      for await (const chunk of source) {
        const bytes = Buffer.from(chunk);
        if (total + bytes.length > MAX_INLINE_ATTACHMENT_BYTES) {
          console.error(
            `photon-sidecar: ${label} exceeds inline cap; forwarding metadata only`
          );
          return meta;
        }
        chunks.push(bytes);
        total += bytes.length;
      }
      meta.data = Buffer.concat(chunks, total).toString("base64");
      meta.encoding = "base64";
    } catch {
      console.error(
        `photon-sidecar: failed to stream ${content.type} bytes; forwarding metadata only`
      );
    }
  }
  return meta;
}

// Best-effort text preview of a reaction's resolved target Message, so the
// Python adapter can populate the gateway's `reply_to_text` (context: WHAT was
// tapped back). The SDK only emits a reaction once it has resolved the full
// target Message (toReactionMessages bails otherwise), so `target.content` is
// hydrated here — no extra round trip. Handles plain text and our patched mixed
// text+attachment groups (first text child); null for attachment/voice-only
// targets. Capped so one long bubble can't balloon the NDJSON line.
const REACTION_TARGET_TEXT_CAP = 2000;
function reactionTargetText(target) {
  const c = target && typeof target === "object" ? target.content : null;
  if (!c || typeof c !== "object") return null;
  let text = null;
  if (c.type === "text") {
    text = c.text;
  } else if (c.type === "group") {
    for (const item of Array.isArray(c.items) ? c.items : []) {
      const ic = item && typeof item === "object" ? item.content : null;
      if (ic && ic.type === "text" && ic.text) {
        text = ic.text;
        break;
      }
    }
  }
  if (typeof text !== "string" || !text) return null;
  return text.length > REACTION_TARGET_TEXT_CAP
    ? text.slice(0, REACTION_TARGET_TEXT_CAP)
    : text;
}

async function normalizeContent(content) {
  if (!content || typeof content !== "object") {
    return { type: "unknown" };
  }
  if (content.type === "text") {
    return { type: "text", text: content.text || "" };
  }
  if (content.type === "attachment" || content.type === "voice") {
    return await normalizeBinaryContent(content);
  }
  if (content.type === "group") {
    const items = [];
    for (const item of Array.isArray(content.items) ? content.items : []) {
      items.push({
        id: item && typeof item === "object" ? item.id ?? null : null,
        content: await normalizeContent(item?.content),
      });
    }
    return { type: "group", items };
  }
  if (content.type === "reaction") {
    const target = content.target;
    return {
      type: "reaction",
      emoji: content.emoji || "",
      targetMessageId: target?.id ?? null,
      // Lets Python gate "is this a reaction to one of MY messages" without
      // tracking every outbound id. May be null if the provider doesn't
      // hydrate the target — Python falls back to its own sent-id cache.
      targetDirection: target?.direction ?? null,
      // Text of the reacted-to message, so Python can correlate the tapback to
      // the gateway's reply_to_text. Null for attachment/voice-only targets.
      targetText: reactionTargetText(target),
    };
  }
  return { type: content.type || "unknown" };
}

async function normalizeEvent(space, message) {
  try {
    const msgSpace = message.space || {};
    const ts = message.timestamp;
    return {
      messageId: message.id ?? null,
      platform: message.platform || space.__platform || "iMessage",
      space: {
        id: space.id ?? msgSpace.id ?? null,
        // iMessage spaces carry `type` ("dm"|"group") and `phone` directly.
        type: space.type ?? msgSpace.type ?? "dm",
        phone: space.phone ?? msgSpace.phone ?? null,
        name:
          space.displayName ??
          space.name ??
          msgSpace.displayName ??
          msgSpace.name ??
          null,
      },
      sender: { id: message.sender ? message.sender.id : null },
      content: await normalizeContent(message.content),
      timestamp: ts instanceof Date ? ts.toISOString() : ts ? String(ts) : null,
    };
  } catch (e) {
    console.error(
      "photon-sidecar: failed to normalize inbound message: " + String(e)
    );
    return null;
  }
}

function inboundStreamErrorMessage(e) {
  const msg = e && e.message ? e.message : String(e);
  let out = "photon-sidecar: inbound stream errored — restarting: " + msg;

  // The Spectrum SDK surfaces Photon cloud CatchUpEvents failures as an
  // iMessage internal error. Local Hermes allowlists cannot cause or fix this:
  // inbound messages stop before they reach the gateway. Add an explicit hint
  // so operators know to retry/restart or escalate to Photon support instead
  // of chasing PHOTON_ALLOWED_USERS / pairing configuration.
  const details = String(e?.cause?.details || e?.details || "");
  const path = String(e?.cause?.path || e?.path || "");
  const code = String(e?.code || "");
  if (
    path.includes("EventService/CatchUpEvents") ||
    details.includes("Unknown server error occurred") ||
    (code === "internalError" && msg.includes("Unknown server error"))
  ) {
    out +=
      " | Photon Spectrum CatchUpEvents returned an internal server error; " +
      "this is upstream of Hermes, so inbound iMessages may not be delivered " +
      "until Photon recovers or the stream is re-established.";
  }
  return out;
}

// spectrum-ts handles in-session gRPC reconnects internally, but if the async
// iterator itself throws or ends, this consumer would stop forever. Wrap it in
// a re-subscribe loop with capped exponential backoff + jitter so inbound
// always recovers (the adapter dedupes any catch-up replay).
(async () => {
  let backoff = 1000;
  for (;;) {
    try {
      for await (const [space, message] of app.messages) {
        backoff = 1000; // healthy traffic — reset
        markStreamHealthy();
        // Only forward inbound messages (ignore our own outbound echoes).
        if (message && message.direction && message.direction !== "inbound") {
          continue;
        }
        rememberInboundSpace(space, message);
        rememberKnownMessage(message);
        const event = await normalizeEvent(space, message);
        if (!event) continue;
        await deliver(JSON.stringify(event));
      }
      console.error("photon-sidecar: inbound stream ended — re-subscribing");
      markStreamRecovering("inbound stream ended");
    } catch (e) {
      const reason = e && e.message ? e.message : String(e);
      console.error(inboundStreamErrorMessage(e));
      markStreamRecovering(reason);
    }
    await new Promise((r) =>
      setTimeout(r, backoff + Math.random() * backoff * 0.2)
    );
    backoff = Math.min(backoff * 2, 30000);
  }
})();

// ---------------------------------------------------------------------------
// HTTP control + inbound server (loopback only).

// Control-message bodies are tiny; cap the body so a compromised local peer
// can't OOM the sidecar by streaming an unbounded request (defence-in-depth on
// the loopback channel).
const MAX_BODY_BYTES = 2 * 1024 * 1024; // 2 MiB
async function readBody(req) {
  const chunks = [];
  let size = 0;
  for await (const chunk of req) {
    size += chunk.length;
    if (size > MAX_BODY_BYTES) {
      req.destroy();
      throw new Error("request body too large");
    }
    chunks.push(chunk);
  }
  const raw = Buffer.concat(chunks).toString("utf-8");
  if (!raw) return {};
  try {
    return JSON.parse(raw);
  } catch (e) {
    throw new Error("invalid JSON body");
  }
}

function unauthorized(res) {
  res.statusCode = 401;
  res.setHeader("Content-Type", "application/json");
  res.end(
    JSON.stringify({
      ok: false,
      error: {
        code: "auth_failed",
        message: "The sidecar authorization token is invalid.",
        retryable: false,
      },
    })
  );
}

function badRequest(res, msg, code = "invalid_request") {
  res.statusCode = code === "not_found" ? 404 : 400;
  res.setHeader("Content-Type", "application/json");
  res.end(
    JSON.stringify({
      ok: false,
      error: { code, message: msg, retryable: false },
    })
  );
}

function classifySafeError(error) {
  const rawCode = String(error?.code || error?.cause?.code || "").toLowerCase();
  const normalizedCode = rawCode.replace(/[^a-z0-9]/g, "");
  const name = String(error?.name || "").toLowerCase();
  const message = String(
    error?.message || error?.cause?.message || error || ""
  );
  const lower = message.toLowerCase();

  if (lower.includes("photon-managed shared line")) {
    return {
      status: 400,
      code: "managed_line_target",
      message: "The target is a Photon-managed sending line, not a recipient.",
      retryable: false,
    };
  }
  if (lower.includes("target not allowed")) {
    return {
      status: 403,
      code: "target_not_allowed",
      message:
        "The recipient must initiate the shared-line conversation first.",
      retryable: false,
    };
  }
  if (
    normalizedCode.includes("resourceexhausted") ||
    normalizedCode.includes("rate") ||
    lower.includes("quota") ||
    lower.includes("rate limit")
  ) {
    return {
      status: 429,
      code: "quota_exceeded",
      message:
        "Photon rejected the operation because a quota or rate limit was reached.",
      retryable: true,
    };
  }
  if (normalizedCode.includes("notfound") || lower.includes("not found")) {
    return {
      status: 404,
      code: "not_found",
      message:
        "The requested Photon message, attachment, or conversation was not found.",
      retryable: false,
    };
  }
  if (
    normalizedCode.includes("operationnotsupported") ||
    name.includes("unsupported") ||
    lower.includes("not supported") ||
    lower.includes("unsupported")
  ) {
    return {
      status: 400,
      code: "unsupported",
      message:
        "Photon does not support this operation for the current line or content.",
      retryable: false,
    };
  }
  if (
    normalizedCode.includes("unauth") ||
    normalizedCode.includes("permission") ||
    name.includes("authentication")
  ) {
    return {
      status: 401,
      code: "auth_failed",
      message:
        "Photon rejected the project credentials or target authorization.",
      retryable: false,
    };
  }
  if (
    normalizedCode.includes("serviceunavailable") ||
    normalizedCode.includes("timeout") ||
    normalizedCode.includes("internal") ||
    lower.includes("temporarily unavailable") ||
    lower.includes("timed out")
  ) {
    return {
      status: 503,
      code: "upstream_unavailable",
      message: "Photon is temporarily unavailable; retry the operation later.",
      retryable: true,
    };
  }
  if (
    normalizedCode.includes("invalidargument") ||
    normalizedCode.includes("preconditionfailed") ||
    (lower.includes("outside") && lower.includes("window"))
  ) {
    return {
      status: 400,
      code: "invalid_request",
      message:
        "Photon rejected the operation because its arguments or state are invalid.",
      retryable: false,
    };
  }
  return {
    status: 500,
    code: "internal_error",
    message: "Photon could not complete the operation.",
    retryable: false,
  };
}

function serverError(res, error, operationId = null) {
  const safe = classifySafeError(error);
  res.statusCode = safe.status;
  res.setHeader("Content-Type", "application/json");
  // Return only curated fields. Provider stacks, identifiers, and credentials
  // never cross the loopback boundary or enter routine handler logs.
  res.end(
    JSON.stringify({
      ok: false,
      error: {
        code: safe.code,
        message: safe.message,
        retryable: safe.retryable,
        ...(operationId ? { operationId } : {}),
      },
    })
  );
}

function ok(res, data) {
  res.statusCode = 200;
  res.setHeader("Content-Type", "application/json");
  res.end(JSON.stringify({ ok: true, ...data }));
}

function safeHeaderValue(value, fallback) {
  const text = String(value || fallback || "attachment")
    .replace(/[\r\n]/g, "_")
    .slice(0, 255);
  return encodeURIComponent(text);
}

function privateAddress(address) {
  if (net.isIPv4(address)) {
    const [a, b] = address.split(".").map(Number);
    return (
      a === 0 ||
      a === 10 ||
      a === 127 ||
      (a === 100 && b >= 64 && b <= 127) ||
      (a === 169 && b === 254) ||
      (a === 172 && b >= 16 && b <= 31) ||
      (a === 192 && b === 168) ||
      (a === 198 && (b === 18 || b === 19)) ||
      a >= 224
    );
  }
  if (net.isIPv6(address)) {
    const lower = address.toLowerCase();
    if (lower.startsWith("::ffff:")) {
      return privateAddress(lower.slice("::ffff:".length));
    }
    return (
      lower === "::" ||
      lower === "::1" ||
      lower.startsWith("fc") ||
      lower.startsWith("fd") ||
      /^fe[89ab]/.test(lower)
    );
  }
  return true;
}

function validHttpUrl(value) {
  try {
    const parsed = new URL(String(value || ""));
    if (parsed.protocol !== "https:") return null;
    if (parsed.username || parsed.password) return null;
    const rawHostname = parsed.hostname.toLowerCase();
    const hostname =
      rawHostname.startsWith("[") && rawHostname.endsWith("]")
        ? rawHostname.slice(1, -1)
        : rawHostname;
    if (
      hostname === "localhost" ||
      hostname.endsWith(".localhost") ||
      hostname.endsWith(".local") ||
      hostname.endsWith(".internal")
    ) {
      return null;
    }
    if (net.isIP(hostname) && privateAddress(hostname)) return null;
    return parsed.toString();
  } catch {
    return null;
  }
}

function normalizeCardLayout(input) {
  if (!input || typeof input !== "object" || Array.isArray(input)) return null;
  const out = {};
  for (const key of [
    "caption",
    "subcaption",
    "trailingCaption",
    "trailingSubcaption",
    "summary",
  ]) {
    if (typeof input[key] === "string" && input[key].length > 1000) return null;
    if (typeof input[key] === "string" && input[key].trim()) {
      out[key] = input[key].trim();
    }
  }
  if (
    (input.imageTitle || input.imageSubtitle) &&
    typeof input.imageBase64 !== "string"
  ) {
    return null;
  }
  if (typeof input.imageBase64 === "string") {
    if (
      input.imageBase64.length > 1_700_000 ||
      typeof input.imageTitle !== "string" ||
      !input.imageTitle.trim() ||
      input.imageTitle.length > 500 ||
      (typeof input.imageSubtitle === "string" &&
        input.imageSubtitle.length > 500)
    ) {
      return null;
    }
    const image = Buffer.from(input.imageBase64, "base64");
    if (
      image.length > 1_250_000 ||
      image.length < 3 ||
      image[0] !== 0xff ||
      image[1] !== 0xd8 ||
      image[2] !== 0xff
    ) {
      return null;
    }
    out.image = image;
    out.imageTitle = input.imageTitle.trim();
    if (typeof input.imageSubtitle === "string" && input.imageSubtitle.trim()) {
      out.imageSubtitle = input.imageSubtitle.trim();
    }
  }
  return Object.keys(out).some((key) => key !== "summary") ? out : null;
}

function handleInbound(req, res) {
  res.statusCode = 200;
  res.setHeader("Content-Type", "application/x-ndjson");
  res.setHeader("Cache-Control", "no-store");
  res.setHeader("Connection", "keep-alive");
  // One consumer at a time — a fresh connection (e.g. after a reconnect)
  // supersedes the previous one.
  if (consumerRes && consumerRes !== res) {
    try {
      consumerRes.end();
    } catch {
      /* ignore */
    }
  }
  setConsumer(res);
  // Heartbeat keeps the socket warm through idle periods and lets the Python
  // side detect a dead pipe promptly.
  const heartbeat = setInterval(() => {
    try {
      res.write("\n");
    } catch {
      /* ignore */
    }
  }, 25000);
  const cleanup = () => {
    clearInterval(heartbeat);
    clearConsumer(res);
  };
  req.on("close", cleanup);
  req.on("aborted", cleanup);
  res.on("error", cleanup);
}

async function resolveSpace(spaceId) {
  if (
    typeof spaceId !== "string" ||
    !spaceId ||
    spaceId.length > 512 ||
    /[\u0000-\u001f]/.test(spaceId)
  ) {
    const error = new Error("invalid space identifier");
    error.code = "invalid_argument";
    throw error;
  }
  const cached = knownSpaces.get(spaceId);
  if (cached) return cached;

  const im = imessage(app);
  const phoneTarget = phoneTargetFromSpaceId(spaceId);
  let space = null;

  // A bare E.164 phone number addresses a DM, so callers can pass just
  // "+1..." (e.g. PHOTON_HOME_CHANNEL for cron delivery) instead of an opaque
  // inbound space id. Photon also represents DM chat ids as `any;-;+1...`;
  // normalize those through the same path. `space.create` accepts the raw
  // phone string directly.
  if (phoneTarget) {
    try {
      space = await im.space.create(phoneTarget);
    } catch (e) {
      console.error(
        "photon-sidecar: phone->DM space.create failed: " +
          (e && e.stack ? e.stack : String(e))
      );
    }
  }
  // Anything else — typically an opaque group GUID — is rehydrated from the
  // persisted id via `space.get`, so group spaces stay reachable after a
  // sidecar restart even before any fresh inbound message in that group.
  if (!space) {
    try {
      space = await im.space.get(spaceId);
    } catch (e) {
      console.error(
        "photon-sidecar: space.get failed: " +
          (e && e.stack ? e.stack : String(e))
      );
    }
  }
  if (!space) throw new Error(`unable to resolve space id ${spaceId}`);

  rememberKnownSpace(spaceId, space);
  if (phoneTarget) rememberKnownSpace(phoneTarget, space);
  rememberKnownSpace(space?.id, space);
  return space;
}

async function resolveMessage(space, messageId) {
  if (
    !messageId ||
    typeof messageId !== "string" ||
    messageId.length > 512 ||
    /[\u0000-\u001f]/.test(messageId)
  ) {
    return null;
  }
  const cached = knownMessages.get(messageId);
  if (cached) return cached;
  const message = await space.getMessage(messageId);
  if (message) rememberKnownMessage(message);
  return message || null;
}

async function sendBuilder(space, builder, replyTo) {
  let outbound = builder;
  if (replyTo) {
    const target = await resolveMessage(space, replyTo);
    if (!target) throw new Error("reply target message not found");
    outbound = spectrumReply(builder, target);
  }
  const result = await space.send(outbound);
  rememberKnownMessage(result);
  if (result?.id) {
    rememberMessageState(result.id, "accepted", { operation: "send" });
  }
  return result;
}

// Constant-time token comparison — don't leak the token via `!==` timing.
const _tokenBuf = Buffer.from(sharedToken);
function tokenOk(header) {
  if (typeof header !== "string") return false;
  const h = Buffer.from(header);
  return h.length === _tokenBuf.length && crypto.timingSafeEqual(h, _tokenBuf);
}

const server = http.createServer(async (req, res) => {
  if (!tokenOk(req.headers["x-hermes-sidecar-token"])) {
    return unauthorized(res);
  }
  // Long-lived inbound NDJSON stream.
  if (req.method === "GET" && req.url === "/inbound") {
    return handleInbound(req, res);
  }
  if (req.method !== "POST") {
    res.statusCode = 405;
    return res.end();
  }
  let body = {};
  try {
    if (req.url === "/healthz") {
      return ok(res, { stream: streamHealthSnapshot() });
    }
    if (req.url === "/shutdown") {
      ok(res, {});
      setTimeout(() => process.kill(process.pid, "SIGTERM"), 50);
      return;
    }
    body = await readBody(req);
    if (req.url === "/attachment") {
      const { attachmentId, phone = null } = body || {};
      if (
        typeof attachmentId !== "string" ||
        !attachmentId ||
        attachmentId.length > 512 ||
        /[\u0000-\u001f]/.test(attachmentId)
      ) {
        return badRequest(res, "a valid attachmentId is required");
      }
      if (phone && !E164_RE.test(phone)) {
        return badRequest(res, "phone must be E.164 when provided");
      }
      const narrowed = imessage(app);
      const item = await narrowed.getAttachment(
        attachmentId,
        phone || undefined
      );
      if (!item) return badRequest(res, "attachment not found", "not_found");
      const declaredSize = Number(item.size);
      if (!Number.isFinite(declaredSize) || declaredSize <= 0) {
        return badRequest(
          res,
          "attachment size is unavailable; refusing an unbounded read"
        );
      }
      if (declaredSize > MAX_FETCH_ATTACHMENT_BYTES) {
        res.statusCode = 413;
        res.setHeader("Content-Type", "application/json");
        return res.end(
          JSON.stringify({
            ok: false,
            error: {
              code: "attachment_too_large",
              message: "The attachment exceeds the configured retrieval limit.",
              retryable: false,
            },
          })
        );
      }
      const source = await item.stream();
      res.statusCode = 200;
      res.setHeader(
        "Content-Type",
        item.mimeType || "application/octet-stream"
      );
      res.setHeader(
        "X-Photon-Filename",
        safeHeaderValue(item.name, "attachment")
      );
      let received = 0;
      for await (const chunk of source) {
        const bytes = Buffer.from(chunk);
        received += bytes.length;
        if (received > MAX_FETCH_ATTACHMENT_BYTES) {
          res.destroy(
            new Error("attachment exceeded configured retrieval limit")
          );
          return;
        }
        if (!res.write(bytes)) await once(res, "drain");
      }
      return res.end();
    }
    if (req.url === "/effect") {
      const {
        spaceId,
        text,
        effect,
        format = "text",
        replyTo = null,
      } = body || {};
      if (!spaceId || typeof text !== "string" || !text || text.length > 8000) {
        return badRequest(res, "spaceId and text are required");
      }
      const effectValue = IMESSAGE_EFFECTS[effect];
      if (!effectValue) {
        return badRequest(res, "effect is not supported", "unsupported");
      }
      if (format !== "text" && format !== "markdown") {
        return badRequest(res, "format must be text or markdown");
      }
      const space = await resolveSpace(spaceId);
      const inner =
        format === "markdown" ? spectrumMarkdown(text) : spectrumText(text);
      const result = await sendBuilder(
        space,
        imessageEffect(inner, effectValue),
        replyTo
      );
      return ok(res, { messageId: result?.id || null });
    }
    if (req.url === "/contact-card") {
      const { spaceId } = body || {};
      if (!spaceId) return badRequest(res, "spaceId is required");
      const space = await resolveSpace(spaceId);
      await space.send(nativeContactCard());
      return ok(res, { state: "accepted" });
    }
    if (req.url === "/custom-app-card") {
      const {
        spaceId,
        appName,
        extensionBundleId,
        teamId,
        url,
        layout,
        appStoreId = null,
        replyTo = null,
      } = body || {};
      const safeUrl = validHttpUrl(url);
      const safeLayout = normalizeCardLayout(layout);
      if (
        !spaceId ||
        typeof appName !== "string" ||
        !appName.trim() ||
        appName.length > 80 ||
        typeof extensionBundleId !== "string" ||
        !/^[A-Za-z0-9.-]+$/.test(extensionBundleId) ||
        !/^[A-Z0-9]{10}$/.test(String(teamId || "")) ||
        !safeUrl ||
        !safeLayout
      ) {
        return badRequest(res, "custom app-card fields are invalid");
      }
      if (
        appStoreId !== null &&
        (!Number.isInteger(appStoreId) || appStoreId <= 0)
      ) {
        return badRequest(res, "appStoreId must be a positive integer");
      }
      const input = {
        appName: appName.trim(),
        extensionBundleId,
        teamId,
        url: safeUrl,
        layout: safeLayout,
      };
      if (appStoreId !== null) input.appStoreId = appStoreId;
      const space = await resolveSpace(spaceId);
      const result = await sendBuilder(
        space,
        customizedMiniApp(input),
        replyTo
      );
      return ok(res, { messageId: result?.id || null });
    }
    if (req.url === "/message-status") {
      const { messageId } = body || {};
      if (!messageId) return badRequest(res, "messageId is required");
      const state = messageStates.get(messageId);
      if (!state)
        return badRequest(res, "message state not found", "not_found");
      return ok(res, { status: state });
    }
    if (req.url === "/recipient-status") {
      const { address } = body || {};
      if (typeof address !== "string" || !E164_RE.test(address)) {
        return badRequest(res, "address must be an E.164 phone number");
      }
      const user = await imessage(app).user(address);
      return ok(res, {
        recipient: {
          service: user?.service || "unknown",
          country: user?.country || null,
        },
      });
    }
    if (req.url === "/send") {
      const { spaceId, text, format = "text", replyTo = null } = body || {};
      if (!spaceId || typeof text !== "string" || text.length > 8000) {
        return badRequest(res, "spaceId and text are required");
      }
      if (format !== "text" && format !== "markdown") {
        return badRequest(res, "format must be text or markdown");
      }
      const space = await resolveSpace(spaceId);
      // iMessage renders markdown natively; spectrum-ts degrades it to
      // readable plain text on platforms that don't.
      const builder =
        format === "markdown" ? spectrumMarkdown(text) : spectrumText(text);
      const result = await sendBuilder(space, builder, replyTo);
      return ok(res, { messageId: result?.id || null });
    }
    if (req.url === "/send-attachment") {
      const {
        spaceId,
        path,
        name,
        mimeType,
        caption,
        kind,
        replyTo = null,
      } = body || {};
      if (!spaceId || typeof path !== "string" || !path) {
        return badRequest(res, "spaceId and path are required");
      }
      const space = await resolveSpace(spaceId);

      // spectrum-ts infers name + MIME from the file extension; pass
      // overrides only when Hermes supplied them so a known-good
      // inference isn't clobbered with an empty string.
      const opts = {};
      if (name) opts.name = name;
      if (mimeType) opts.mimeType = mimeType;
      const builder =
        kind === "voice"
          ? voice(path, Object.keys(opts).length ? opts : undefined)
          : attachment(path, Object.keys(opts).length ? opts : undefined);

      const result = await sendBuilder(space, builder, replyTo);

      // iMessage delivers the caption as a separate bubble; send it
      // after the media so the attachment renders first.
      if (caption && typeof caption === "string" && caption.length <= 8000) {
        try {
          await space.send(spectrumText(caption));
        } catch (e) {
          console.error(
            "photon-sidecar: attachment sent but caption failed: " +
              (e && e.stack ? e.stack : String(e))
          );
        }
      }
      return ok(res, { messageId: result?.id || null });
    }
    if (req.url === "/edit") {
      const { spaceId, messageId, text } = body || {};
      if (
        !spaceId ||
        !messageId ||
        typeof text !== "string" ||
        text.length > 8000
      ) {
        return badRequest(res, "spaceId, messageId and text are required");
      }
      const space = await resolveSpace(spaceId);
      // Spectrum rehydrates getMessage() results as inbound wrappers, which
      // cannot be edited. Only live outbound handles from this process are valid.
      const target = knownMessages.get(messageId);
      if (!target) return badRequest(res, "message not found", "not_found");
      await space.send(spectrumEdit(spectrumText(text), target));
      rememberMessageState(messageId, "edited", { operation: "edit" });
      return ok(res, { messageId });
    }
    if (req.url === "/unsend") {
      const { spaceId, messageId } = body || {};
      if (!spaceId || !messageId) {
        return badRequest(res, "spaceId and messageId are required");
      }
      const space = await resolveSpace(spaceId);
      const target = knownMessages.get(messageId);
      if (!target) return badRequest(res, "message not found", "not_found");
      await space.send(spectrumUnsend(target));
      knownMessages.delete(messageId);
      rememberMessageState(messageId, "unsent", { operation: "unsend" });
      return ok(res, { messageId });
    }
    if (req.url === "/react") {
      const { spaceId, messageId, emoji } = body || {};
      if (!spaceId || !messageId || typeof emoji !== "string" || !emoji) {
        return badRequest(res, "spaceId, messageId and emoji are required");
      }
      const space = await resolveSpace(spaceId);
      const target =
        knownMessages.get(messageId) ?? (await space.getMessage(messageId));
      if (!target) {
        return badRequest(res, "message not found", "not_found");
      }
      const handle = await target.react(emoji);
      if (!handle) {
        return badRequest(
          res,
          "reactions not supported on this platform",
          "unsupported"
        );
      }
      lruSet(
        reactionHandles,
        `${spaceId}\u0000${messageId}`,
        { emoji, handle },
        MAX_REACTION_HANDLES
      );
      return ok(res, { reactionId: handle.id ?? null });
    }
    if (req.url === "/unreact") {
      const { spaceId, messageId, reactionId } = body || {};
      if (!spaceId || !messageId) {
        return badRequest(res, "spaceId and messageId are required");
      }
      const key = `${spaceId}\u0000${messageId}`;
      const slot = reactionHandles.get(key);
      if (slot) {
        await slot.handle.unsend();
        reactionHandles.delete(key);
        return ok(res, {});
      }
      // Restart-recovery: the live handle is gone, so try rehydrating the
      // reaction message by id and retracting it. Only outbound messages can
      // be unsent — if the provider rehydrates it as inbound (or not at all)
      // this throws, and that's an expected soft failure, not a sidecar bug:
      // a stale tapback self-heals when the next /react replaces it.
      if (reactionId) {
        try {
          const space = await resolveSpace(spaceId);
          const msg = await space.getMessage(reactionId);
          if (msg) {
            await space.unsend(msg);
            return ok(res, {});
          }
        } catch (e) {
          console.error(
            "photon-sidecar: best-effort unreact failed: " +
              (e && e.message ? e.message : String(e))
          );
        }
        return badRequest(res, "reaction not removable", "not_found");
      }
      return badRequest(res, "no tracked reaction for message", "not_found");
    }
    if (req.url === "/read") {
      const { spaceId, messageId } = body || {};
      if (!spaceId || !messageId) {
        return badRequest(res, "spaceId and messageId are required");
      }
      const space = await resolveSpace(spaceId);
      const target =
        knownMessages.get(messageId) ?? (await space.getMessage(messageId));
      if (!target) {
        return badRequest(res, "message not found", "not_found");
      }
      await target.read();
      return ok(res, {});
    }
    if (req.url === "/typing") {
      const { spaceId, state = "start" } = body || {};
      if (!spaceId) return badRequest(res, "spaceId is required");
      if (state !== "start" && state !== "stop") {
        return badRequest(res, "state must be start or stop");
      }
      const space = await resolveSpace(spaceId);
      await space.send(spectrumTyping(state));
      return ok(res, {});
    }
    return badRequest(res, "sidecar endpoint not found", "not_found");
  } catch (e) {
    const safe = classifySafeError(e);
    console.error(`photon-sidecar: handler error: ${safe.code}`);
    const operationId =
      typeof body?.messageId === "string" && body.messageId
        ? body.messageId
        : `failed:${crypto.randomUUID()}`;
    rememberMessageState(operationId, "failed", {
      operation: String(req.url || "unknown").replace(/^\//, ""),
      errorCode: safe.code,
    });
    return serverError(res, e, operationId);
  }
});

server.listen(port, bind, () => {
  console.error(`photon-sidecar: listening on ${bind}:${port}`);
});

let stopping = false;
async function shutdown(signal) {
  // Re-entry guard: stdin EOF, a signal and /shutdown can all fire together
  // during one teardown.
  if (stopping) return;
  stopping = true;
  console.error(`photon-sidecar: received ${signal}, stopping...`);
  try {
    await Promise.race([
      app.stop(),
      new Promise((resolve) => setTimeout(resolve, 3000)),
    ]);
  } catch (e) {
    console.error("photon-sidecar: app.stop() failed: " + String(e));
  }
  server.close(() => process.exit(0));
  setTimeout(() => process.exit(1), 500).unref();
}

process.on("SIGINT", () => shutdown("SIGINT"));
process.on("SIGTERM", () => shutdown("SIGTERM"));

// Lifetime binding to the parent. The adapter spawns us with stdin as a pipe
// it holds open; EOF means the gateway process is gone — including hard
// deaths (crash, SIGKILL) where no signal and no /shutdown ever reaches us.
// Without this, an orphaned sidecar squats the port and keeps consuming the
// inbound gRPC stream, and every replacement spawn dies on EADDRINUSE.
// Opt-in via env so manual `node index.mjs` runs aren't affected.
if (process.env.PHOTON_SIDECAR_WATCH_STDIN === "1") {
  process.stdin.resume();
  process.stdin.on("end", () => shutdown("stdin EOF (parent exited)"));
  process.stdin.on("error", () => shutdown("stdin error (parent exited)"));
}

// Don't let a stray promise rejection take the process down silently — handlers
// catch their own errors, so log and keep serving (Python supervises restart on
// a real fatal exit).
process.on("unhandledRejection", (reason) => {
  console.error(
    "photon-sidecar: unhandledRejection: " +
      (reason && reason.stack ? reason.stack : String(reason))
  );
});
