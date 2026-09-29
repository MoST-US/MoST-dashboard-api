const fs = require("node:fs/promises");
const os = require("node:os");
const path = require("node:path");
const http = require("node:http");
const https = require("node:https");
const express = require("express");
const cors = require("cors");
const dotenv = require("dotenv");
const { parse } = require("csv-parse/sync");
const { spawn } = require("node:child_process");

const projectRoot = path.resolve(__dirname, process.env.MOST_PROJECT_ROOT || "..");
dotenv.config({ path: path.join(projectRoot, ".env") });
dotenv.config();

const app = express();
const port = Number(process.env.PORT || 4000);

app.use(cors());
app.use(express.json());

const resultsRoot = path.resolve(projectRoot, process.env.RESULTS_DIR || "results");
const DEFAULT_RESULTS_SCOPE = "current";
const SLURM_LOG_PATTERN = /^slurm-(\d+)\.out$/;
const SQUEUE_TIMEOUT_MS = 10000;
const MERGE_RESULTS_TIMEOUT_MS = Number(process.env.MERGED_RESULTS_TIMEOUT || 120) * 1000;

function toPosixRelative(targetPath) {
  return path.relative(projectRoot, targetPath).split(path.sep).join("/");
}

function ensureWithinBase(base, candidate) {
  const resolved = path.resolve(candidate);
  const normalizedBase = path.resolve(base);
  if (resolved === normalizedBase || resolved.startsWith(normalizedBase + path.sep)) {
    return resolved;
  }
  throw new Error("Invalid path.");
}

function safeJoin(base, ...parts) {
  return ensureWithinBase(base, path.join(base, ...parts));
}

async function listDirectories(dir) {
  const entries = await fs.readdir(dir, { withFileTypes: true });
  return entries.filter((entry) => entry.isDirectory()).map((entry) => entry.name);
}

async function listExperimentFolders(basePath) {
  try {
    const names = await listDirectories(basePath);
    return names
      .filter((name) => !name.startsWith(".") && name !== "__pycache__")
      .sort((a, b) => a.localeCompare(b));
  } catch {
    return [];
  }
}

async function listIterations(experiment, basePath) {
  const experimentPath = safeJoin(basePath, experiment);
  const names = await listDirectories(experimentPath);
  return names.filter((name) => !name.startsWith(".")).sort((a, b) => b.localeCompare(a));
}

async function findLatestIterationFolder(basePath) {
  const experiments = await listExperimentFolders(basePath);
  const candidates = [];

  for (const experiment of experiments) {
    if (/^MST[-_]\d+$/i.test(experiment)) {
      continue;
    }

    const iterations = await listIterations(experiment, basePath).catch(() => []);
    for (const iteration of iterations) {
      const iterationPath = safeJoin(basePath, experiment, iteration);
      const stat = await fs.stat(iterationPath).catch(() => null);
      if (stat) {
        candidates.push({
          experiment,
          iteration,
          path: iterationPath,
          modifiedAt: stat.mtimeMs,
        });
      }
    }
  }

  candidates.sort((left, right) => right.modifiedAt - left.modifiedAt);
  return candidates.length > 0 ? candidates[0] : null;
}

async function findCurrentExperiment(basePath) {
  return findLatestIterationFolder(basePath);
}

function normalizeResultsScope(rawScope) {
  const normalized = String(rawScope || DEFAULT_RESULTS_SCOPE).trim();
  if (!normalized || normalized.toLowerCase() === DEFAULT_RESULTS_SCOPE) {
    return DEFAULT_RESULTS_SCOPE;
  }

  if (!/^[A-Za-z0-9._-]+$/.test(normalized)) {
    const error = new Error("Invalid results scope.");
    error.code = "INVALID_SCOPE";
    throw error;
  }

  return normalized;
}

async function resolveResultsBasePath(scope) {
  if (scope === DEFAULT_RESULTS_SCOPE) {
    return resultsRoot;
  }

  const scopedPath = safeJoin(resultsRoot, scope);
  const stat = await fs.stat(scopedPath);
  if (!stat.isDirectory()) {
    const error = new Error("Results scope not found.");
    error.code = "ENOENT";
    throw error;
  }

  return scopedPath;
}

function getResultsScopeFromRequest(req) {
  return normalizeResultsScope(
    req.query.resultsScope || req.query.resultsSet || req.query.round || DEFAULT_RESULTS_SCOPE,
  );
}

async function listResultsScopes() {
  const directoryNames = await listDirectories(resultsRoot).catch(() => []);
  const detectedRoundScopes = directoryNames
    .filter((name) => /^Experiment_/i.test(name))
    .sort((a, b) => a.localeCompare(b, undefined, { numeric: true, sensitivity: "base" }));

  return [DEFAULT_RESULTS_SCOPE, ...detectedRoundScopes];
}

function stripQuotes(value) {
  return String(value || "").trim().replace(/^['\"]|['\"]$/g, "");
}

function parseCsvRows(csvText) {
  try {
    return {
      rows: parse(csvText, {
        columns: true,
        skip_empty_lines: true,
        trim: true,
        bom: true,
      }),
      relaxed: false,
    };
  } catch {
    // Some exported CSV files contain inconsistent columns or quotes.
    // Retry with tolerant settings so dashboards can still consume most rows.
    return {
      rows: parse(csvText, {
        columns: true,
        skip_empty_lines: true,
        trim: true,
        bom: true,
        relax_column_count: true,
        relax_quotes: true,
      }),
      relaxed: true,
    };
  }
}

function getRequestedCsvFields(req) {
  const rawFields = req.query.fields;
  if (rawFields === undefined) {
    return null;
  }

  const fields = String(rawFields)
    .split(",")
    .map((field) => field.trim())
    .filter(Boolean);

  if (fields.length === 0) {
    const error = new Error("The fields parameter cannot be empty.");
    error.code = "INVALID_FIELDS";
    throw error;
  }

  return [...new Set(fields)];
}

function selectCsvFields(rows, requestedFields) {
  if (!requestedFields) {
    return rows;
  }

  return rows.map((row) => {
    const selected = {};
    for (const field of requestedFields) {
      if (Object.prototype.hasOwnProperty.call(row, field)) {
        selected[field] = row[field];
      }
    }
    return selected;
  });
}

// Additive (`WORKLOAD_MIXES`) experiments are not tied to a token-interval matrix cell: their
// experiment folder is named after the canonical mix (parent_dir:
// `mix_<in_min>-<in_max>_<out_min>-<out_max>@<alpha>+...`, the label's ':' replaced by '_'), the
// four MIN/MAX_INPUT/OUTPUT_TOKENS columns are intentionally empty in results.csv, and the
// experiment is described by the WORKLOAD_MIX / ADDITIVE*_PROPORTIONS columns instead. Additive
// detection is therefore cheap: the folder name starts with `mix_`, or the row carries
// ADDITIVE=TRUE / a non-empty WORKLOAD_MIX. The rendering mirrors experiment_environment's
// workload_mix.py::_build_mix so the API reports the same canonical mix the environment wrote.
const ADDITIVE_EXPERIMENT_PREFIX = "mix_";
const ADDITIVE_ALPHA_DECIMALS = 6;
const ADDITIVE_TOKEN_RANGE_RE = /^(\d+)(?:-(\d+))?$/;
const ADDITIVE_PROFILE_PAIR_RE = /\(([^()]*)\)/g;

function parseAdditiveAlpha(rawAlpha) {
  const numeric = Number(String(rawAlpha == null ? "" : rawAlpha).trim());
  return Number.isFinite(numeric) && numeric > 0 ? numeric : null;
}

// Same compact rendering as workload_mix.py::_format_alpha (0.5, 0.333333, 1).
function formatAdditiveAlpha(alpha) {
  const text = Number(alpha)
    .toFixed(ADDITIVE_ALPHA_DECIMALS)
    .replace(/0+$/, "")
    .replace(/\.$/, "");
  return text || "0";
}

function parseAdditiveTokenRange(text) {
  const match = ADDITIVE_TOKEN_RANGE_RE.exec(String(text == null ? "" : text).trim());
  if (!match) {
    return null;
  }

  const first = Number(match[1]);
  const second = match[2] === undefined ? first : Number(match[2]);
  if (first <= 0 || second <= 0) {
    return null;
  }

  return { min: Math.min(first, second), max: Math.max(first, second) };
}

// Normalises the alphas and derives the canonical mix string and the mix envelope, the same way
// workload_mix.py::_build_mix does.
function buildAdditiveMix(profiles) {
  const total = profiles.reduce((sum, profile) => sum + profile.alpha, 0);
  if (!(total > 0)) {
    return null;
  }

  const normalized = profiles.map((profile) => ({ ...profile, alpha: profile.alpha / total }));
  const canonical = `[${normalized
    .map((profile) => `(${profile.label},${formatAdditiveAlpha(profile.alpha)})`)
    .join(",")}]`;

  return {
    canonical,
    profiles: normalized,
    envelope: {
      inMin: Math.min(...normalized.map((profile) => profile.inMin)),
      inMax: Math.max(...normalized.map((profile) => profile.inMax)),
      outMin: Math.min(...normalized.map((profile) => profile.outMin)),
      outMax: Math.max(...normalized.map((profile) => profile.outMax)),
    },
  };
}

// Parses an experiment folder name such as `mix_1-100_1-100@0.5+300-600_100-300@0.5`.
// Returns null when the name is not an additive mix folder or a part is malformed.
function parseWorkloadMixName(experimentName) {
  const name = String(experimentName == null ? "" : experimentName).trim();
  if (!name.toLowerCase().startsWith(ADDITIVE_EXPERIMENT_PREFIX)) {
    return null;
  }

  const body = name.slice(ADDITIVE_EXPERIMENT_PREFIX.length);
  if (!body) {
    return null;
  }

  const profiles = [];
  for (const rawPart of body.split("+")) {
    const part = rawPart.trim();
    const atIndex = part.lastIndexOf("@");
    if (atIndex <= 0) {
      return null;
    }

    const alpha = parseAdditiveAlpha(part.slice(atIndex + 1));
    const intervalParts = part.slice(0, atIndex).split("_");
    if (alpha === null || intervalParts.length !== 2) {
      return null;
    }

    const inRange = parseAdditiveTokenRange(intervalParts[0]);
    const outRange = parseAdditiveTokenRange(intervalParts[1]);
    if (!inRange || !outRange) {
      return null;
    }

    profiles.push({
      label: `${inRange.min}-${inRange.max}:${outRange.min}-${outRange.max}`,
      inMin: inRange.min,
      inMax: inRange.max,
      outMin: outRange.min,
      outMax: outRange.max,
      alpha,
    });
  }

  return profiles.length > 0 ? buildAdditiveMix(profiles) : null;
}

// Parses the WORKLOAD_MIX column, e.g. `[(1-100:1-100,0.5),(300-600:100-300,0.5)]`.
function parseCanonicalMix(rawValue) {
  const text = String(rawValue == null ? "" : rawValue).trim();
  if (!text) {
    return null;
  }

  const profiles = [];
  ADDITIVE_PROFILE_PAIR_RE.lastIndex = 0;
  let match;
  while ((match = ADDITIVE_PROFILE_PAIR_RE.exec(text)) !== null) {
    const pair = match[1];
    const commaIndex = pair.lastIndexOf(",");
    if (commaIndex <= 0) {
      return null;
    }

    const alpha = parseAdditiveAlpha(pair.slice(commaIndex + 1));
    const intervalParts = pair.slice(0, commaIndex).trim().split(":");
    if (alpha === null || intervalParts.length !== 2) {
      return null;
    }

    const inRange = parseAdditiveTokenRange(intervalParts[0]);
    const outRange = parseAdditiveTokenRange(intervalParts[1]);
    if (!inRange || !outRange) {
      return null;
    }

    profiles.push({
      label: `${inRange.min}-${inRange.max}:${outRange.min}-${outRange.max}`,
      inMin: inRange.min,
      inMax: inRange.max,
      outMin: outRange.min,
      outMax: outRange.max,
      alpha,
    });
  }

  return profiles.length > 0 ? buildAdditiveMix(profiles) : null;
}

// Parses the ADDITIVE_*_PROPORTIONS columns (`{"1-100:1-100":0.5,...}`); null when empty/invalid.
function parseAdditiveProportions(rawValue) {
  const text = String(rawValue == null ? "" : rawValue).trim();
  if (!text) {
    return null;
  }

  try {
    const parsed = JSON.parse(text);
    if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
      return null;
    }

    const entries = Object.entries(parsed)
      .map(([label, value]) => [String(label).trim(), Number(value)])
      .filter(([label, value]) => label && Number.isFinite(value));
    return entries.length > 0 ? Object.fromEntries(entries) : null;
  } catch {
    return null;
  }
}

function isAdditiveRow(row) {
  if (!row || typeof row !== "object") {
    return false;
  }

  const flag = firstNonEmpty(row.ADDITIVE, row.additive);
  if (flag && ["true", "1", "yes"].includes(flag.toLowerCase())) {
    return true;
  }

  return Boolean(firstNonEmpty(row.WORKLOAD_MIX, row.workload_mix));
}

// Descriptor for additive experiments: detection plus the mix metadata and the expected / observed
// proportions. Falls back from the row columns to the experiment folder name, so it works even when
// the requested `fields` filter strips the additive columns from the returned rows.
function buildAdditiveDescriptor(experiment, rows) {
  const fromName = parseWorkloadMixName(experiment);
  const row = Array.isArray(rows) && rows.length > 0 ? rows[0] : null;
  const fromRow = row ? parseCanonicalMix(firstNonEmpty(row.WORKLOAD_MIX, row.workload_mix)) : null;
  const isAdditive = Boolean(fromName || fromRow || isAdditiveRow(row));

  if (!isAdditive) {
    return { isAdditive: false };
  }

  const mix = fromRow || fromName;
  return {
    isAdditive: true,
    canonical: mix ? mix.canonical : null,
    profiles: mix ? mix.profiles : [],
    envelope: mix ? mix.envelope : null,
    expectedProportions: row
      ? parseAdditiveProportions(
          firstNonEmpty(row.ADDITIVE_EXPECTED_PROPORTIONS, row.additive_expected_proportions),
        )
      : null,
    trueProportions: row
      ? parseAdditiveProportions(
          firstNonEmpty(row.ADDITIVE_TRUE_PROPORTIONS, row.additive_true_proportions),
        )
      : null,
  };
}

function inferGpuFromUrl(rawUrl) {
  const cleaned = stripQuotes(rawUrl);
  if (!cleaned) {
    return null;
  }

  let urlToParse = cleaned;
  if (!/^https?:\/\//i.test(urlToParse)) {
    urlToParse = `http://${urlToParse}`;
  }

  try {
    const parsed = new URL(urlToParse);
    const host = parsed.hostname;
    if (!host) {
      return null;
    }
    const firstLabel = host.split(".")[0];
    return firstLabel || host;
  } catch {
    return cleaned;
  }
}

function inferModelFromUrl(rawUrl) {
  const cleaned = stripQuotes(rawUrl);
  if (!cleaned) {
    return null;
  }

  let urlToParse = cleaned;
  if (!/^https?:\/\//i.test(urlToParse)) {
    return null;
  }

  try {
    const parsed = new URL(urlToParse);
    const parts = parsed.pathname
      .split("/")
      .map((part) => part.trim())
      .filter((part) => part && !["v1", "chat", "completions", "generate", "models"].includes(part));
    return parts.length > 0 ? parts[parts.length - 1] : null;
  } catch {
    return null;
  }
}

function normalizeEndpointBaseUrl(rawValue) {
  const cleaned = stripQuotes(rawValue);
  if (!cleaned) {
    return null;
  }

  try {
    const parsed = new URL(cleaned);
    if (parsed.protocol && parsed.host) {
      return `${parsed.protocol}//${parsed.host}`;
    }
  } catch {
    // Ignore and retry by assuming the scheme is missing.
  }

  if (cleaned.includes(" ") || cleaned.startsWith("/")) {
    return null;
  }

  try {
    const parsedWithScheme = new URL(`http://${cleaned}`);
    return `${parsedWithScheme.protocol}//${parsedWithScheme.host}`;
  } catch {
    return null;
  }
}

async function queryModelFromEndpoint(rawEndpoint, timeoutMs = 10000) {
  const baseUrl = normalizeEndpointBaseUrl(rawEndpoint);
  if (!baseUrl) {
    return null;
  }

  let payload;
  try {
    payload = await new Promise((resolve, reject) => {
      const target = new URL("/v1/models", `${baseUrl}/`);
      const transport = target.protocol === "https:" ? https : http;
      const request = transport.request(
        target,
        {
          method: "GET",
          headers: {
            Accept: "application/json",
            "User-Agent": "most-api/1.0",
          },
        },
        (response) => {
          const statusCode = Number(response.statusCode || 0);
          if (statusCode !== 200) {
            response.resume();
            resolve(null);
            return;
          }

          let raw = "";
          response.setEncoding("utf8");
          response.on("data", (chunk) => {
            raw += chunk;
          });
          response.on("end", () => {
            try {
              resolve(JSON.parse(raw));
            } catch {
              resolve(null);
            }
          });
        },
      );

      request.setTimeout(timeoutMs, () => {
        request.destroy(new Error("Request timed out"));
      });

      request.on("error", reject);
      request.end();
    });
  } catch {
    return null;
  }

  if (!payload || typeof payload !== "object" || !Array.isArray(payload.data) || payload.data.length === 0) {
    return null;
  }

  const first = payload.data[0];
  if (!first || typeof first !== "object") {
    return null;
  }

  const modelId = first.id;
  if (typeof modelId === "string" && modelId.trim()) {
    return modelId.trim();
  }

  return null;
}

function deepExtractModel(value) {
  if (!value || typeof value !== "object") {
    return null;
  }

  if (!Array.isArray(value)) {
    if (typeof value.model === "string" && value.model.trim()) {
      return value.model.trim();
    }
    if (typeof value.model_name === "string" && value.model_name.trim()) {
      return value.model_name.trim();
    }
  }

  if (Array.isArray(value)) {
    for (const item of value) {
      const found = deepExtractModel(item);
      if (found) {
        return found;
      }
    }
    return null;
  }

  for (const nested of Object.values(value)) {
    const found = deepExtractModel(nested);
    if (found) {
      return found;
    }
  }

  return null;
}

async function tryReadJson(filePath) {
  try {
    const text = await fs.readFile(filePath, "utf8");
    return JSON.parse(text);
  } catch {
    return null;
  }
}

async function tryReadCsvRecords(filePath) {
  try {
    const text = await fs.readFile(filePath, "utf8");
    return parseCsvRows(text).rows;
  } catch {
    return null;
  }
}

function firstNonEmpty(...values) {
  for (const value of values) {
    if (typeof value === "string" && value.trim()) {
      return value.trim();
    }
  }
  return null;
}

function normalizeFailureValue(value) {
  if (value == null) {
    return null;
  }

  const text = String(value).trim().toLowerCase();
  if (!text) {
    return null;
  }

  if (["failed", "fail", "error", "errored", "timeout", "timed_out", "false", "0"].includes(text)) {
    return false;
  }

  if (["passed", "pass", "success", "ok", "true", "1", "completed", "done"].includes(text)) {
    return true;
  }

  if (text.includes("fail")) {
    return false;
  }

  if (text.includes("pass") || text.includes("success")) {
    return true;
  }

  return null;
}

function deepExtractFailureStatus(value) {
  if (!value || typeof value !== "object") {
    return null;
  }

  if (Array.isArray(value)) {
    for (const item of value) {
      const found = deepExtractFailureStatus(item);
      if (found !== null) {
        return found;
      }
    }
    return null;
  }

  for (const key of ["status", "result", "outcome", "success", "passed", "failed", "state"]) {
    if (key in value) {
      const normalized = normalizeFailureValue(value[key]);
      if (normalized !== null) {
        return normalized;
      }
    }
  }

  for (const nested of Object.values(value)) {
    const found = deepExtractFailureStatus(nested);
    if (found !== null) {
      return found;
    }
  }

  return null;
}

async function evaluateIterationFailure(experiment, iteration, basePath) {
  const iterationDir = safeJoin(basePath, experiment, iteration);

  try {
    const jsonPath = path.join(iterationDir, "results.json");
    const jsonText = await fs.readFile(jsonPath, "utf8");
    const jsonData = JSON.parse(jsonText);
    const parsed = deepExtractFailureStatus(jsonData);
    if (parsed !== null) {
      return parsed;
    }
  } catch {
    // Ignore JSON parse failures and fall back to CSV inspection.
  }

  try {
    const csvPath = path.join(iterationDir, "results.csv");
    const csvText = await fs.readFile(csvPath, "utf8");
    const rows = parseCsvRows(csvText).rows;

    for (const row of rows) {
      for (const [key, value] of Object.entries(row)) {
        if (/status|result|outcome|success|pass|fail|error|state/i.test(key)) {
          const normalized = normalizeFailureValue(value);
          if (normalized !== null) {
            return normalized;
          }
        }
      }

      for (const value of Object.values(row)) {
        const normalized = normalizeFailureValue(value);
        if (normalized !== null) {
          return normalized;
        }
      }
    }
  } catch {
    // Ignore CSV parsing failures.
  }

  return null;
}

async function getLastFourIterationsFailureStatus(experiment, basePath) {
  const iterations = await listIterations(experiment, basePath).catch(() => []);
  const recent = iterations.slice(0, 4);

  const statuses = [];
  for (const iteration of recent) {
    const failed = await evaluateIterationFailure(experiment, iteration, basePath);
    const normalizedStatus = failed === null ? "unknown" : failed ? "passed" : "failed";
		statuses.push({
      iteration,
      failed: failed === false,
      passed: failed === true,
      status: normalizedStatus,
    });
  }

  const lastFourFailed = recent.length >= 4 && statuses.every((item) => item.failed === true);

  return {
    experiment,
    checkedIterations: recent,
    window: 4,
    statuses,
    lastFourFailed,
    allKnown: statuses.every((item) => item.status !== "unknown"),
  };
}

async function resolveLlmName() {
  const timeoutMs = Number(process.env.MODEL_DISCOVERY_TIMEOUT || 10) * 1000;
  const endpointCandidates = [
    process.env.URL,
    process.env.FMPERF_ENDPOINT_URL,
    process.env.ENDPOINT_URL,
  ];

  for (const endpoint of endpointCandidates) {
    const modelFromEndpoint = await queryModelFromEndpoint(endpoint, timeoutMs);
    if (modelFromEndpoint) {
      return {
        llmName: modelFromEndpoint,
        source: "env:/v1/models",
      };
    }
  }

  return { llmName: "unknown", source: "unavailable" };
}

async function resolveGpuUsed(resultsBasePath) {
  const envCandidate = firstNonEmpty(
    process.env.GPU,
    process.env.GPU_USED,
    process.env.GPU_NAME,
    process.env.ACCELERATOR,
  );
  if (envCandidate) {
    return { gpuUsed: envCandidate, source: "env" };
  }

  const latest = await findLatestIterationFolder(resultsBasePath);
  if (latest) {
    const csvRecords = await tryReadCsvRecords(path.join(latest.path, "results.csv"));
    if (csvRecords && csvRecords.length > 0) {
      const row = csvRecords[0];
      const gpuFromCsv = firstNonEmpty(
        row.GPU_COUNT,
        row.gpu_count,
        row.GPU,
        row.gpu,
        row.GPU_USED,
        row.gpu_used,
        row.HARDWARE,
        row.hardware,
      );
      if (gpuFromCsv) {
        return {
          gpuUsed: gpuFromCsv,
          source: `results/${latest.experiment}/${latest.iteration}/results.csv`,
        };
      }
    }
  }

  const gpuFromUrl = inferGpuFromUrl(process.env.URL);
  if (gpuFromUrl) {
    return { gpuUsed: gpuFromUrl, source: "env:URL" };
  }

  return { gpuUsed: "unknown", source: "unavailable" };
}

async function requireExistingFile(filePath) {
  await fs.access(filePath);
  return filePath;
}

async function listSlurmLogs(basePath) {
  const entries = await fs.readdir(basePath, { withFileTypes: true }).catch(() => []);
  const logs = entries
    .filter((entry) => entry.isFile())
    .map((entry) => {
      const match = SLURM_LOG_PATTERN.exec(entry.name);
      if (!match) {
        return null;
      }
      const jobId = Number(match[1]);
      if (!Number.isInteger(jobId) || jobId < 0) {
        return null;
      }
      return {
        name: entry.name,
        jobId,
        path: path.join(basePath, entry.name),
      };
    })
    .filter((log) => log !== null)
    .sort((left, right) => right.jobId - left.jobId);

  return logs;
}

async function findLatestSlurmLog(basePath) {
  const logs = await listSlurmLogs(basePath);
  return logs.length > 0 ? logs[0] : null;
}

function runCommandCaptureStdout(command, args, timeoutMs = SQUEUE_TIMEOUT_MS) {
  return new Promise((resolve) => {
    let child;
    try {
      child = spawn(command, args, {
        stdio: ["ignore", "pipe", "pipe"],
        timeout: timeoutMs,
      });
    } catch {
      resolve(null);
      return;
    }

    let stdout = "";
    child.stdout.setEncoding("utf8");
    child.stdout.on("data", (chunk) => {
      stdout += chunk;
    });

    const fail = () => resolve(null);
    child.on("error", fail);
    child.on("timeout", fail);
    child.on("close", (code) => {
      resolve(code === 0 ? stdout : null);
    });
  });
}

async function getRunningSlurmJobIds() {
  const stdout = await runCommandCaptureStdout("squeue", ["--noheader", "--format=%i"]);
  if (stdout === null) {
    return null;
  }

  const jobIds = new Set();
  for (const line of stdout.split(/\r?\n/)) {
    const match = /^\s*(\d+)/.exec(line);
    if (match) {
      jobIds.add(Number(match[1]));
    }
  }
  return jobIds;
}

function resolveGpuCountScriptPath() {
  return path.join(
    projectRoot,
    "fmperf",
    "utils",
    "GpuCount.py",
  );
}

function runPythonCaptureJson(scriptPath, args, timeoutMs = SQUEUE_TIMEOUT_MS) {
  return new Promise((resolve) => {
    let child;
    try {
      child = spawn(process.env.PYTHON || "python", [scriptPath, ...args], {
        stdio: ["ignore", "pipe", "pipe"],
        timeout: timeoutMs,
      });
    } catch {
      resolve(null);
      return;
    }

    let stdout = "";
    child.stdout.setEncoding("utf8");
    child.stdout.on("data", (chunk) => {
      stdout += chunk;
    });

    let stderr = "";
    child.stderr.setEncoding("utf8");
    child.stderr.on("data", (chunk) => {
      stderr += chunk;
    });

    const fail = () => resolve(null);
    child.on("error", fail);
    child.on("timeout", fail);
    child.on("close", (code) => {
      let data = null;
      try {
        data = JSON.parse(stdout.trim());
      } catch {
        // stdout was not JSON; treat as an unparseable outcome below.
      }
      resolve({ exited: code, data, stderr });
    });
  });
}

async function findModelJobGpuCount({ modelId, node, port }) {
  const scriptPath = resolveGpuCountScriptPath();
  const captured = await runPythonCaptureJson(scriptPath, [
    "find",
    "--model",
    modelId,
    "--node",
    node,
    "--port",
    port,
  ]);

  if (!captured || !captured.data) {
    const error = new Error("The GPU count helper is unavailable.");
    error.code = "GPU_HELPER_UNAVAILABLE";
    throw error;
  }

  if (!captured.data.error) {
    return captured.data;
  }

  if (captured.data.code === "SQUEUE_UNAVAILABLE") {
    const error = new Error(captured.data.error);
    error.code = "SQUEUE_UNAVAILABLE";
    throw error;
  }

  const error = new Error(captured.data.error);
  error.code = "JOB_NOT_FOUND";
  throw error;
}

function resolveMergeResultsScriptPath() {
  return path.join(
    projectRoot,
    "fmperf",
    "utils",
    "MergeResultsCsv.py",
  );
}

function parseExperimentList(rawExperiments) {
  const values = Array.isArray(rawExperiments) ? rawExperiments : [rawExperiments];
  const experiments = [];

  for (const value of values) {
    for (const token of String(value || "").split(",")) {
      const name = token.trim();
      if (name && !experiments.includes(name)) {
        experiments.push(name);
      }
    }
  }

  return experiments;
}

async function removeTempDirectory(tempDir) {
  if (!tempDir) {
    return;
  }
  await fs.rm(tempDir, { recursive: true, force: true }).catch(() => {});
}

async function buildMergedResultsCsv(basePath, experiments) {
  const tempDir = await fs.mkdtemp(path.join(os.tmpdir(), "most-merged-results-"));
  const csvPath = path.join(tempDir, "merged-results.csv");
  const args = ["merge", "--root", basePath, "--output", csvPath];
  if (experiments.length > 0) {
    args.push("--experiments", experiments.join(","));
  }

  const captured = await runPythonCaptureJson(
    resolveMergeResultsScriptPath(),
    args,
    MERGE_RESULTS_TIMEOUT_MS,
  );

  if (captured && captured.exited === 0 && !(captured.data && captured.data.error)) {
    return { tempDir, csvPath };
  }

  await removeTempDirectory(tempDir);

  if (captured && captured.stderr && captured.stderr.trim()) {
    console.error(`Merged CSV helper failed: ${captured.stderr.trim()}`);
  }

  const helperError = captured && captured.data ? captured.data : null;
  const error = new Error(
    (helperError && helperError.error) || "The merged CSV helper is unavailable.",
  );
  error.code =
    helperError && helperError.code === "NO_RESULTS_FOUND"
      ? "NO_RESULTS_FOUND"
      : "MERGE_HELPER_UNAVAILABLE";
  throw error;
}

async function resolveGpuCountFromResults(req) {
  const resultsScope = getResultsScopeFromRequest(req);
  const basePath = await resolveResultsBasePath(resultsScope);
  const latest = await findLatestIterationFolder(basePath);
  if (!latest) {
    return null;
  }

  const csvRecords = await tryReadCsvRecords(path.join(latest.path, "results.csv"));
  if (!csvRecords || csvRecords.length === 0) {
    return null;
  }

  const row = csvRecords[0];
  const rawCount = firstNonEmpty(row.GPU_COUNT, row.gpu_count, row.GPUS, row.gpus);
  if (!rawCount) {
    return null;
  }

  const numeric = Number(rawCount);
  return {
    gpuCount: Number.isInteger(numeric) ? numeric : rawCount,
    source: "results/" + latest.experiment + "/" + latest.iteration + "/results.csv",
    resultsScope,
  };
}

async function resolveExperimentStatus(basePath) {
  const latest = await findLatestSlurmLog(basePath);
  const runningJobIds = await getRunningSlurmJobIds();

  let isRunning = null;
  if (runningJobIds && latest) {
    isRunning = runningJobIds.has(latest.jobId);
  }

  return {
    isRunning,
    slurmJobId: latest ? latest.jobId : null,
    logFile: latest ? latest.name : null,
    logAvailable: Boolean(latest),
    runningJobIds: runningJobIds ? [...runningJobIds].sort((a, b) => a - b) : null,
    squeueAvailable: Boolean(runningJobIds),
  };
}

async function readSlurmLogTail(filePath, maxLines) {
  const handle = await fs.open(filePath, "r");
  try {
    const lines = [];
    let dropped = 0;
    for await (const line of handle.readLines()) {
      if (lines.length >= maxLines) {
        lines.shift();
        dropped += 1;
      }
      lines.push(line);
    }
    return { lines, dropped };
  } finally {
    await handle.close();
  }
}

app.get("/health", (_req, res) => {
  res.json({ ok: true, service: "most-api" });
});

app.get("/api/llm-name", async (_req, res) => {
  const data = await resolveLlmName();
  res.json(data);
});

app.get("/api/gpu-used", async (req, res, next) => {
  try {
    const resultsScope = getResultsScopeFromRequest(req);
    const basePath = await resolveResultsBasePath(resultsScope);
    const data = await resolveGpuUsed(basePath);
    res.json({
      ...data,
      url: firstNonEmpty(
        process.env.URL,
        process.env.FMPERF_ENDPOINT_URL,
        process.env.ENDPOINT_URL,
      ),
      resultsScope,
      resultsRoot: toPosixRelative(basePath),
    });
  } catch (error) {
    next(error);
  }
});

app.get("/api/results-scopes", async (_req, res) => {
  const scopes = await listResultsScopes();
  res.json({
    scopes,
    defaultScope: DEFAULT_RESULTS_SCOPE,
  });
});

app.get("/api/experiments", async (_req, res, next) => {
  try {
    const resultsScope = getResultsScopeFromRequest(_req);
    const basePath = await resolveResultsBasePath(resultsScope);
    const experiments = await listExperimentFolders(basePath);
    // `experiments` stays a plain list of folder names for backward compatibility; the additive
    // (WORKLOAD_MIXES) subset is described separately, decoded from the `mix_...` folder name.
    const additiveExperiments = experiments
      .map((name) => {
        const mix = parseWorkloadMixName(name);
        if (!mix) {
          return null;
        }

        return {
          name,
          isAdditive: true,
          canonical: mix.canonical,
          profiles: mix.profiles,
          envelope: mix.envelope,
        };
      })
      .filter(Boolean);

    res.json({
      resultsRoot: toPosixRelative(basePath),
      resultsScope,
      experiments,
      additiveExperiments,
    });
  } catch (error) {
    next(error);
  }
});

app.get("/api/current-experiment", async (req, res, next) => {
  try {
    const resultsScope = getResultsScopeFromRequest(req);
    const basePath = await resolveResultsBasePath(resultsScope);
    const current = await findCurrentExperiment(basePath);

    res.json({
      resultsScope,
      experiment: current ? current.experiment : null,
      iteration: current ? current.iteration : null,
      source: current ? toPosixRelative(current.path) : null,
      isAvailable: Boolean(current),
    });
  } catch (error) {
    next(error);
  }
});

app.get("/api/experiment-status", async (_req, res, next) => {
  try {
    const data = await resolveExperimentStatus(projectRoot);
    res.json(data);
  } catch (error) {
    next(error);
  }
});

app.get("/api/experiment-log", async (req, res, next) => {
  try {
    const latest = await findLatestSlurmLog(projectRoot);
    if (!latest) {
      const error = new Error("No slurm log files found.");
      error.code = "NO_SLURM_LOG";
      throw error;
    }

    const rawLines = req.query.lines;
    let requestedLines = null;
    if (rawLines !== undefined) {
      const parsed = Number(rawLines);
      if (!Number.isInteger(parsed) || parsed < 1) {
        const error = new Error("The lines parameter must be a positive integer.");
        error.code = "INVALID_LINES";
        throw error;
      }
      requestedLines = parsed;
    }

    const logInfo = {
      slurmJobId: latest.jobId,
      logFile: latest.name,
      logPath: toPosixRelative(latest.path),
    };

    if (requestedLines === null) {
      const content = await fs.readFile(latest.path, "utf8");
      res.json({
        ...logInfo,
        truncated: false,
        requestedLines: null,
        content,
      });
      return;
    }

    const { lines, dropped } = await readSlurmLogTail(latest.path, requestedLines);
    res.json({
      ...logInfo,
      truncated: dropped > 0,
      requestedLines,
      returnedLines: lines.length,
      content: lines.join("\n"),
    });
  } catch (error) {
    next(error);
  }
});

app.get("/api/job-gpu-count", async (req, res, next) => {
  try {
    const modelId = String(req.query.model || "").trim();
    const node = String(req.query.node || "").trim();
    const port = String(req.query.port || "").trim();

    if (!modelId || !node || !port) {
      const error = new Error("The model, node and port query parameters are required.");
      error.code = "INVALID_JOB_QUERY";
      throw error;
    }

    if (!/^\d+$/.test(port)) {
      const error = new Error("The port query parameter must be a number.");
      error.code = "INVALID_JOB_QUERY";
      throw error;
    }

    try {
      const data = await findModelJobGpuCount({ modelId, node, port });
      res.json(data);
      return;
    } catch (jobError) {
      // The job may have ended or Slurm may be unavailable; fall back to the
      // GPU_COUNT recorded in the latest results.csv of the requested scope.
      if (jobError && ["JOB_NOT_FOUND", "SQUEUE_UNAVAILABLE", "GPU_HELPER_UNAVAILABLE"].includes(jobError.code)) {
        const fromResults = await resolveGpuCountFromResults(req);
        if (fromResults) {
          res.json({
            ...fromResults,
            model: modelId,
            node,
            port,
            inspectionError: jobError.message,
            inspectionCode: jobError.code,
          });
          return;
        }
      }
      throw jobError;
    }
  } catch (error) {
    next(error);
  }
});

app.get("/api/experiments/:experiment/iterations", async (req, res, next) => {
  try {
    const { experiment } = req.params;
    const resultsScope = getResultsScopeFromRequest(req);
    const basePath = await resolveResultsBasePath(resultsScope);
    const iterations = await listIterations(experiment, basePath);
    res.json({ experiment, iterations, resultsScope });
  } catch (error) {
    next(error);
  }
});

app.get("/api/experiments/:experiment/last-four-failed", async (req, res, next) => {
  try {
    const { experiment } = req.params;
    const resultsScope = getResultsScopeFromRequest(req);
    const basePath = await resolveResultsBasePath(resultsScope);
    const data = await getLastFourIterationsFailureStatus(experiment, basePath);

    res.json({
      experiment,
      resultsScope,
      ...data,
    });
  } catch (error) {
    next(error);
  }
});

app.get("/api/experiments/:experiment/iterations/:iteration/results.csv", async (req, res, next) => {
  try {
    const { experiment, iteration } = req.params;
    const resultsScope = getResultsScopeFromRequest(req);
    const basePath = await resolveResultsBasePath(resultsScope);
    const csvPath = await requireExistingFile(
      safeJoin(basePath, experiment, iteration, "results.csv"),
    );
    const csvText = await fs.readFile(csvPath, "utf8");
    let parsed;

    try {
      parsed = parseCsvRows(csvText);
    } catch (parseError) {
      parseError.code = "CSV_PARSE_FAILED";
      parseError.message = "Unable to parse results.csv.";
      throw parseError;
    }

    const requestedFields = getRequestedCsvFields(req);
    const rows = selectCsvFields(parsed.rows, requestedFields);
    // Additive metadata is derived from the full parsed rows (before field filtering) so it is
    // still reported when `?fields=` strips the WORKLOAD_MIX / ADDITIVE*_PROPORTIONS columns.
    const additive = buildAdditiveDescriptor(experiment, parsed.rows);

    res.json({
      experiment,
      iteration,
      resultsScope,
      fields: requestedFields,
      rows,
      count: rows.length,
      relaxedParsing: parsed.relaxed,
      source: toPosixRelative(csvPath),
      additive,
    });
  } catch (error) {
    next(error);
  }
});

app.get("/api/experiments/:experiment/iterations/:iteration/download/results.csv", async (req, res, next) => {
  try {
    const { experiment, iteration } = req.params;
    const resultsScope = getResultsScopeFromRequest(req);
    const basePath = await resolveResultsBasePath(resultsScope);
    const csvPath = await requireExistingFile(
      safeJoin(basePath, experiment, iteration, "results.csv"),
    );
    res.download(csvPath, "results.csv");
  } catch (error) {
    next(error);
  }
});

app.get("/api/experiments/:experiment/iterations/:iteration/download/results.json", async (req, res, next) => {
  try {
    const { experiment, iteration } = req.params;
    const resultsScope = getResultsScopeFromRequest(req);
    const basePath = await resolveResultsBasePath(resultsScope);
    const jsonPath = await requireExistingFile(
      safeJoin(basePath, experiment, iteration, "results.json"),
    );
    res.download(jsonPath, "results.json");
  } catch (error) {
    next(error);
  }
});

app.get("/api/experiments/download/merged-results.csv", async (req, res, next) => {
  let tempDir = null;

  try {
    const resultsScope = getResultsScopeFromRequest(req);
    const basePath = await resolveResultsBasePath(resultsScope);
    const experiments = parseExperimentList(req.query.experiments);
    const merged = await buildMergedResultsCsv(basePath, experiments);
    tempDir = merged.tempDir;

    res.download(merged.csvPath, "matrix-results-merged.csv", (error) => {
      removeTempDirectory(tempDir);
      if (error && !res.headersSent) {
        next(error);
      }
    });
  } catch (error) {
    await removeTempDirectory(tempDir);
    next(error);
  }
});

app.use((error, _req, res, _next) => {
  let status = 500;
  let message = "Unexpected server error.";

  if (error && (error.code === "ENOENT" || error.message === "Invalid path.")) {
    status = 404;
    message = "Resource not found.";
  } else if (error && error.code === "INVALID_SCOPE") {
    status = 400;
    message = "Invalid results scope.";
  } else if (error && error.code === "INVALID_FIELDS") {
    status = 400;
    message = error.message;
  } else if (error && error.code === "CSV_PARSE_FAILED") {
    status = 422;
    message = "The CSV file exists but could not be parsed.";
  } else if (error && error.code === "NO_SLURM_LOG") {
    status = 404;
    message = error.message;
  } else if (error && error.code === "INVALID_LINES") {
    status = 400;
    message = error.message;
  } else if (error && error.code === "INVALID_JOB_QUERY") {
    status = 400;
    message = error.message;
  } else if (error && error.code === "SQUEUE_UNAVAILABLE") {
    status = 503;
    message = error.message;
  } else if (error && error.code === "JOB_NOT_FOUND") {
    status = 404;
    message = error.message;
  } else if (error && error.code === "NO_RESULTS_FOUND") {
    status = 404;
    message = error.message;
  } else if (error && error.code === "MERGE_HELPER_UNAVAILABLE") {
    status = 503;
    message = error.message;
  }

  res.status(status).json({ error: message });
});

app.listen(port, () => {
  console.log(`MoST API listening on http://localhost:${port}`);
  console.log(`Reading experiments from: ${resultsRoot}`);
});
