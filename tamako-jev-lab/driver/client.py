"""Backends for the Jev decisions endpoint.

Two implementations with one interface:

    decide(state, questions, gold=None) -> {
        "answers": {qid: answer dict},
        "usage": {"input_tokens": int, "output_tokens": int, "cost": float?} | None,
        "cost": float | None,        # usage.cost when the provider reports it
        "model": str | None,         # response model field (version audit)
        "latency_s": float,
        "error": str | None,
    }

`OpenRouterBackend` hits https://openrouter.ai/api/alpha/decisions.
`MockBackend` produces deterministic synthetic answers from the task's gold
labels, so the full benchmark pipeline (runner, metrics, report) can be
verified offline with `--mock`.

Ported from jev-demo jev_bench/client.py, 2026-09-18.
Adaptations vs the source:
  - the response `model` field is recorded in the result (version audit);
  - `usage.cost` is lifted into a top-level `cost` key when present;
  - `requests` is imported lazily so `--mock` runs on hosts without it
    (this bench box has no requests / no network credentials).
"""

from __future__ import annotations

import hashlib
import random
import time

try:
    import requests
except ModuleNotFoundError:  # offline --mock hosts without requests installed
    requests = None

OPENROUTER_DECISIONS_URL = "https://openrouter.ai/api/alpha/decisions"
DEFAULT_MODEL = "typesafe/jev-1.13"


class OpenRouterBackend:
    def __init__(
        self,
        api_key: str,
        model: str = DEFAULT_MODEL,
        timeout_s: float = 120.0,
        max_attempts: int = 6,
        referer: str | None = None,
        title: str | None = None,
    ):
        if requests is None:
            raise RuntimeError(
                "requests 未安装：真实后端需要 `pip install requests`；离线自验请用 --mock"
            )
        self.api_key = api_key
        self.model = model
        self.timeout_s = timeout_s
        self.max_attempts = max_attempts
        self.session = requests.Session()
        headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        }
        if referer:
            headers["HTTP-Referer"] = referer
        if title:
            headers["X-OpenRouter-Title"] = title
        self.session.headers.update(headers)

    def decide(self, state, questions, gold=None):
        payload = {"model": self.model, "state": state, "questions": questions}
        start = time.monotonic()
        last_err = None
        for attempt in range(self.max_attempts):
            try:
                resp = self.session.post(
                    OPENROUTER_DECISIONS_URL, json=payload, timeout=self.timeout_s
                )
                if resp.status_code == 429 or resp.status_code >= 500:
                    retry_after = resp.headers.get("Retry-After")
                    wait = (
                        float(retry_after)
                        if retry_after
                        else min(2.0**attempt, 30.0) + random.random()
                    )
                    last_err = f"HTTP {resp.status_code}: {resp.text[:300]}"
                    time.sleep(wait)
                    continue
                if resp.status_code != 200:
                    return {
                        "answers": {},
                        "usage": None,
                        "cost": None,
                        "model": None,
                        "latency_s": time.monotonic() - start,
                        "error": f"HTTP {resp.status_code}: {resp.text[:500]}",
                    }
                body = resp.json()
                usage = body.get("usage")
                return {
                    "answers": body.get("answers", {}),
                    "usage": usage,
                    "cost": (usage or {}).get("cost"),
                    "model": body.get("model"),
                    "latency_s": time.monotonic() - start,
                    "error": None,
                }
            except requests.RequestException as exc:
                last_err = f"{type(exc).__name__}: {exc}"
                time.sleep(min(2.0**attempt, 30.0) + random.random())
        return {
            "answers": {},
            "usage": None,
            "cost": None,
            "model": None,
            "latency_s": time.monotonic() - start,
            "error": f"exhausted retries: {last_err}",
        }


class MockBackend:
    """Deterministic fake Jev. Correct with probability `skill` per question.

    Uses gold labels to synthesize plausible answer objects (including
    probability distributions and confidence) so every metric path is
    exercised offline. Never makes network calls.
    """

    def __init__(self, seed: int = 0, skill: float = 0.9):
        self.seed = seed
        self.skill = skill

    def _rng(self, state, qid, salt=""):
        key = f"{self.seed}|{salt}|{qid}|{repr(state)}"
        return random.Random(int(hashlib.sha256(key.encode()).hexdigest(), 16))

    def decide(self, state, questions, gold=None):
        start = time.monotonic()
        answers = {}
        q_salt = repr(questions)
        for qid, q in questions.items():
            g = (gold or {}).get(qid)
            rng = self._rng(state, qid, salt=q_salt)
            correct = rng.random() < self.skill
            if q["type"] == "noul":
                target = bool(g) if g is not None else rng.random() < 0.5
                if not correct:
                    target = not target
                p = rng.uniform(0.8, 0.99) if target else rng.uniform(0.01, 0.2)
                answers[qid] = {"type": "noul", "noul": round(p, 4)}
            elif q["type"] == "choice":
                options = list(q["criteria"].keys())
                pick = g if (g in options and correct) else rng.choice(options)
                if g in options and not correct:
                    others = [o for o in options if o != g]
                    pick = rng.choice(others)
                top = rng.uniform(0.7, 0.95)
                rest = [rng.random() for _ in options]
                tot = sum(rest) or 1.0
                probs = {}
                for o, r in zip(options, rest):
                    probs[o] = round(top if o == pick else (1 - top) * r / tot, 4)
                answers[qid] = {
                    "type": "choice",
                    "choice": pick,
                    "probabilities": probs,
                    "confidence": round(top - (1 - top) / max(len(options) - 1, 1), 4),
                }
            elif q["type"] == "score":
                n = len(q["criteria"])
                base = float(g) if g is not None else rng.uniform(0, n - 1)
                if not correct:
                    base = float(rng.randrange(n))
                score = min(max(base + rng.uniform(-0.3, 0.3), 0.0), n - 1.0)
                answers[qid] = {
                    "type": "score",
                    "score": round(score, 3),
                    "legend": {str(i): c for i, c in enumerate(q["criteria"])},
                    "confidence": round(rng.uniform(0.6, 0.95), 4),
                }
        return {
            "answers": answers,
            "usage": None,
            "cost": None,
            "model": "mock",
            "latency_s": time.monotonic() - start,
            "error": None,
        }
