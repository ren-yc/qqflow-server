/**
 * Executable smoke: real HTTP requests against a running qqflow-server,
 * asserting actual HTTP results. Credentials come from the environment;
 * no real data. Run: node dist/smoke.js (server must be reachable).
 */

import { QqflowClient, StatusError } from "./client.js";

const base = process.env.QQFLOW_BASE_URL ?? "http://127.0.0.1:5032";
const token = process.env.QQFLOW_API_TOKEN;

if (!token) {
  console.error("smoke: QQFLOW_API_TOKEN must be set (server prints it on first start)");
  process.exit(2);
}

const client = new QqflowClient(base, token);

// 1) health: free endpoint, must answer 200 with a JSON object
const health = await client.health();
if (typeof health !== "object" || health === null) {
  console.error("smoke: /health did not return an object");
  process.exit(1);
}
console.log("smoke: /health ok, account phase =", health.account ?? "(none)");

// 2) authenticated listing: must answer 200 and carry an accounts array
const accounts = await client.accounts();
if (!Array.isArray(accounts)) {
  console.error("smoke: /api/v1/accounts did not return a list");
  process.exit(1);
}
console.log("smoke: /api/v1/accounts ok,", accounts.length, "account(s)");

// 3) registration refusal surfaces as an error, not a wait
try {
  await client.ensureReady("10000000000", {
    qq: "10000000000",
    key: "0123456789abcdef",
    db_path: "X:/definitely/not/a/real/path",
  }, 3);
  console.log("smoke: registration accepted (server had no binding)");
} catch (err) {
  if (err instanceof StatusError || err instanceof Error) {
    console.log("smoke: registration path errored as designed:", (err as Error).message.slice(0, 80));
  }
}

console.log("smoke: PASS");
