"""jev-memory benchmark driver: fixtures -> tasks -> backend -> report.

Usage:
    python3 run.py --fixtures DIR [--suites all|uc1a,uc3,...] [--limit N]
                   [--mock] [--out DIR] [--concurrency 8] [--seed 0]
                   [--skip-reference]

Flow: build tasks from fixtures -> run MockBackend or OpenRouterBackend ->
(optional) purpose-model reference judgments (reference_cache.json,
idempotent) -> aggregate metrics -> report.json + raw.jsonl.

Orchestration/output ported from jev-demo run_benchmark.py +
jev_bench/runner.py, 2026-09-18.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import random
import re
import sys
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import metrics as M
from client import DEFAULT_MODEL, MockBackend, OpenRouterBackend
from suites import (SUITE_BUILDERS, SUITE_GROUPS, FixturesMissing,
                    build_suite, check_fixtures_dir)

NOUL_SUITES = {"uc1a", "uc1b", "uc1s", "uc3", "uc3s", "uc4", "zh_en"}
# UC-5: 中文校准聚合池（UC-1/UC-3 家族的全部 noul 套件，合成套件带构造标签）
UC5_POOL = ("uc1a", "uc1b", "uc1s", "uc3", "uc3s", "uc4")


# ---------------------------------------------------------------------------
# execution (ported from jev-demo jev_bench/runner.py)
# ---------------------------------------------------------------------------

def run_tasks(tasks, backend, concurrency=8):
    def one(task):
        res = backend.decide(task["state"], task["questions"], gold=task.get("gold"))
        return {
            "id": task["id"],
            "suite": task["suite"],
            "meta": task["meta"],
            "gold": task["gold"],
            "answers": res["answers"],
            "usage": res.get("usage"),
            "cost": res.get("cost"),
            "model": res.get("model"),
            "latency_s": res["latency_s"],
            "error": res["error"],
        }

    rows = []
    with ThreadPoolExecutor(max_workers=concurrency) as pool:
        for row in pool.map(one, tasks):
            rows.append(row)
    return rows


def _ok(rows):
    return [r for r in rows if r["error"] is None and r["answers"]]


def _get_answer(row, qid="q"):
    return row["answers"].get(qid) or {}


def _pred_of(row, qid="q"):
    a = _get_answer(row, qid)
    t = a.get("type")
    if t == "noul":
        return a.get("noul")
    if t == "choice":
        return a.get("choice")
    if t == "score":
        return a.get("score")
    return None


# ---------------------------------------------------------------------------
# reference judgments (optional; skipped under --mock / --skip-reference)
# ---------------------------------------------------------------------------

def attach_reference(suite, tasks, rows, judge, concurrency=8):
    """Fill row['reference'] / row['labels'] from the purpose-model judge.
    Per-task failures degrade to ref_error (labels stay absent)."""
    ok_ids = {r["id"] for r in _ok(rows)}
    todo = [t for t in tasks if t["id"] in ok_ids]
    by_id = {}

    def one(task):
        meta = task["meta"]
        try:
            if suite in ("uc1a", "uc1b", "zh_en"):
                same = judge.confirm_same(task["state"])
                return task["id"], {"labels": {"q": same}, "raw": {"same": same}}
            if suite == "uc2":
                verdict = judge.judge_merge(task["state"])
                return task["id"], {"labels": {"q": verdict}, "raw": {"verdict": verdict}}
            if suite in ("uc3", "uc3s", "uc4"):
                texts = meta.get("candidate_texts") or []
                prompt = meta.get("prompt_text", task["state"])
                selected = judge.gate_select(prompt, texts)
                if suite == "uc4":
                    return task["id"], {
                        "labels": {"q": bool(selected)},
                        "raw": {"selected": selected},
                    }
                labels = {f"c{n}": True for n in selected}
                return task["id"], {"labels": labels, "raw": {"selected": selected}}
        except Exception as exc:  # reference 失败不拖垮整轮：该行仅缺标签
            return task["id"], {"labels": None, "ref_error": f"{type(exc).__name__}: {exc}"}
        return task["id"], None

    with ThreadPoolExecutor(max_workers=concurrency) as pool:
        for tid, res in pool.map(one, todo):
            if res is not None:
                by_id[tid] = res
    for row in rows:
        res = by_id.get(row["id"])
        if res:
            row["reference"] = res


def effective_labels(row):
    """构造标签（合成套件）优先，否则 reference 标签。"""
    if row.get("gold"):
        return row["gold"]
    ref = row.get("reference") or {}
    return ref.get("labels")


# ---------------------------------------------------------------------------
# aggregation
# ---------------------------------------------------------------------------

def _ref_labels(row):
    return (row.get("reference") or {}).get("labels")


def _noul_points(rows, label_fn=None):
    """(prob, gold_float|None) across every noul question of every ok row.
    label_fn 默认 effective_labels（构造/人工金标优先，其次参照标签）。"""
    label_fn = label_fn or effective_labels
    pts = []
    for r in _ok(rows):
        labels = label_fn(r) or {}
        for qid, a in r["answers"].items():
            if a.get("type") != "noul":
                continue
            g = labels.get(qid)
            pts.append((a.get("noul"), None if g is None else float(bool(g))))
    return pts


def _calibration_block(pts):
    """Labeled -> ECE/Brier/AUROC etc.; unlabeled -> probability quantiles."""
    probs = [p for p, _ in pts if p is not None]
    labeled = [(p, g) for p, g in pts if p is not None and g is not None]
    out = {"n_questions": len(pts), "n_labeled": len(labeled)}
    if labeled:
        lp = [p for p, _ in labeled]
        lg = [g for _, g in labeled]
        out["accuracy_at_0.5"] = M.accuracy([p > 0.5 for p in lp], [bool(g) for g in lg])
        out["brier"] = M.brier_binary(lp, lg)
        out["log_loss"] = M.log_loss_binary(lp, lg)
        out["ece"] = M.ece_binary(lp, lg)
        out["auroc"] = M.auroc(lp, lg)
        out["reliability"] = M.reliability_bins(lp, lg)
    elif probs:
        out["prob_distribution"] = M.latency_stats(probs)  # 分位数复用
    return out


def _injection_alignment(rows, expected_key):
    """uc3/uc3s: 选中边（p>0.5）∩ 期望边（injected / buried）的对齐命中。"""
    n_windows = 0
    tp = fp = fn = 0
    for r in _ok(rows):
        meta = r["meta"]
        ids = meta.get("candidate_edge_ids") or []
        expected = set(meta.get(expected_key) or [])
        selected = set()
        for i, eid in enumerate(ids):
            p = (_get_answer(r, f"c{i + 1}")).get("noul")
            if p is not None and p > 0.5:
                selected.add(eid)
        hits = selected & expected
        tp += len(hits)
        fp += len(selected - expected)
        fn += len(expected - selected)
        n_windows += 1
    return {
        "n_windows": n_windows,
        "n_selected": tp + fp,
        "n_expected": tp + fn,
        "n_hits": tp,
        "precision": tp / (tp + fp) if tp + fp else None,
        "recall": tp / (tp + fn) if tp + fn else None,
    }


def aggregate(suite, rows):
    agg = {
        "n_tasks": len(rows),
        "n_errors": sum(r["error"] is not None or not r["answers"] for r in rows),
    }
    ok = _ok(rows)
    costs = [r["cost"] for r in ok if r.get("cost") is not None]
    if costs:
        agg["cost_total"] = sum(costs)
    tokens_in = [r["usage"]["input_tokens"] for r in ok
                 if r.get("usage") and r["usage"].get("input_tokens")]
    tokens_out = [r["usage"]["output_tokens"] for r in ok
                  if r.get("usage") and r["usage"].get("output_tokens")]
    if tokens_in:
        agg["input_tokens_total"] = sum(tokens_in)
    if tokens_out:
        agg["output_tokens_total"] = sum(tokens_out)
    models = sorted({r["model"] for r in ok if r.get("model")})
    if models:
        agg["models"] = models  # 版本审计
    lat = M.latency_stats([r["latency_s"] for r in rows])
    if lat:
        agg["latency"] = lat
    n_ref = sum(1 for r in ok if (r.get("reference") or {}).get("labels"))
    if n_ref:
        agg["n_reference_labeled"] = n_ref
    n_gold = sum(1 for r in rows if r.get("gold"))
    agg["gold_coverage"] = (n_gold / len(rows)) if rows else None

    if suite in NOUL_SUITES:
        if n_gold:
            # 有金标：主指标严格只用金标任务；参照标签单独出参照指标
            gold_rows = [r for r in rows if r.get("gold")]
            agg.update(_calibration_block(
                _noul_points(gold_rows, lambda r: r["gold"])))
            ref_pts = _noul_points(rows, _ref_labels)
            if any(g is not None for _, g in ref_pts):
                agg["reference_metrics"] = _calibration_block(ref_pts)
        else:
            # 无金标：维持现状（参照标签作标签，皆无则概率分位数）
            agg.update(_calibration_block(_noul_points(rows)))
    if suite == "uc1s":
        by_cat = defaultdict(list)
        for r in ok:
            p = _pred_of(r)
            g = (effective_labels(r) or {}).get("q")
            if p is not None and g is not None:
                by_cat[r["meta"].get("category", "?")].append((p > 0.5, bool(g)))
        agg["accuracy_by_category"] = {
            c: M.accuracy([p for p, _ in v], [g for _, g in v])
            for c, v in sorted(by_cat.items())
        }

    if suite == "uc2":
        agg["label_distribution"] = M.label_distribution(
            [p for p in (_pred_of(r) for r in ok) if p is not None])
        gold_rows = [r for r in ok if r.get("gold")]
        # 有金标：主指标严格只用金标任务；无金标：维持现状（参照标签）
        src = gold_rows or ok
        label_fn = (lambda r: r["gold"]) if gold_rows else effective_labels
        labeled = []
        for r in src:
            p = _pred_of(r)
            g = (label_fn(r) or {}).get("q")
            if p is not None and g is not None:
                labeled.append((r, p, g))
        if labeled:
            lp = [p for _, p, _ in labeled]
            lg = [g for _, _, g in labeled]
            agg["accuracy"] = M.accuracy(lp, lg)
            agg["macro_f1"] = M.macro_f1(lp, lg)
            confs = [_get_answer(r).get("confidence") for r, _, _ in labeled]
            correct = [p == g for _, p, g in labeled]
            agg["ece_top_label"] = M.ece_multiclass(confs, correct)
        if gold_rows:
            ref_labeled = [(_pred_of(r), (_ref_labels(r) or {}).get("q"))
                           for r in ok]
            ref_labeled = [(p, g) for p, g in ref_labeled
                           if p is not None and g is not None]
            if ref_labeled:
                agg["reference_metrics"] = {
                    "n_labeled": len(ref_labeled),
                    "accuracy": M.accuracy([p for p, _ in ref_labeled],
                                           [g for _, g in ref_labeled]),
                    "macro_f1": M.macro_f1([p for p, _ in ref_labeled],
                                           [g for _, g in ref_labeled]),
                }

    if suite == "uc3":
        agg["injection_alignment"] = _injection_alignment(rows, "injected_edge_ids")
    if suite == "uc3s":
        agg["injection_alignment"] = _injection_alignment(rows, "expected_edge_ids")
    if suite == "uc4":
        # 与上游注入事实对齐：pred_any vs 实际有注入
        preds, golds = [], []
        for r in ok:
            p = _pred_of(r)
            if p is not None:
                preds.append(p > 0.5)
                golds.append(bool(r["meta"].get("injected_edge_ids")))
        acc = M.accuracy(preds, golds)
        if acc is not None:
            agg["alignment_accuracy_vs_injected"] = acc

    return agg


def zh_en_alignment(rows_by_suite):
    """zh_en vs uc1a：同一对 zh/en 判定的翻转率与平均概率差。"""
    zh = {r["id"]: _pred_of(r) for r in _ok(rows_by_suite.get("uc1a", []))}
    groups = []
    for r in _ok(rows_by_suite.get("zh_en", [])):
        zh_p = zh.get(r["meta"].get("zh_task_id"))
        en_p = _pred_of(r)
        if zh_p is not None and en_p is not None:
            groups.append((zh_p, en_p))
    if not groups:
        return None
    return {
        "n_pairs": len(groups),
        "flip_rate_at_0.5": M.flip_rate([[z > 0.5, e > 0.5] for z, e in groups]),
        "mean_pairwise_dev": M.mean_pairwise_dev([[z, e] for z, e in groups]),
        "n_translated": sum(1 for r in _ok(rows_by_suite.get("zh_en", []))
                            if r["meta"].get("translated")),
    }


# ---------------------------------------------------------------------------
# suite resolution / outputs (ported from jev-demo runner.py)
# ---------------------------------------------------------------------------

def resolve_suites(spec):
    if spec == "all":
        return list(SUITE_BUILDERS)
    out = []
    for part in spec.split(","):
        part = part.strip()
        if part in SUITE_GROUPS:
            out.extend(SUITE_GROUPS[part])
        else:
            out.append(part)
    return out


def write_outputs(out_dir, report, rows):
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    (out / "report.json").write_text(
        json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")
    with (out / "raw.jsonl").open("w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")
    return out


def print_summary(report):
    for name, agg in report["suites"].items():
        line = f"  {name:<10} n={agg['n_tasks']:<4} errors={agg['n_errors']}"
        if agg.get("gold_coverage") is not None:
            line += f"  gold={agg['gold_coverage']:.0%}"
        for key in ("accuracy", "accuracy_at_0.5", "ece", "brier", "auroc",
                    "macro_f1", "cost_total"):
            if agg.get(key) is not None and not isinstance(agg[key], (dict, list)):
                line += f"  {key}={agg[key]:.3f}"
        al = agg.get("injection_alignment")
        if al and al.get("n_hits") is not None:
            line += f"  hits={al['n_hits']}/{al['n_expected']}"
        print(line)
    uc5 = report.get("uc5_zh_calibration")
    if uc5:
        line = f"  {'uc5(zh校准)':<10} n_q={uc5.get('n_questions', 0):<4} labeled={uc5.get('n_labeled', 0)}"
        for key in ("accuracy_at_0.5", "ece", "brier", "auroc"):
            if uc5.get(key) is not None:
                line += f"  {key}={uc5[key]:.3f}"
        print(line)


# ---------------------------------------------------------------------------
# ---------------------------------------------------------------------------
# human gold labels (--gold) and annotation export (--dump-unlabeled)
# ---------------------------------------------------------------------------

def load_gold_csv(path):
    """CSV（表头 task_id,label）-> {task_id: label}。空 label 行忽略。"""
    mapping = {}
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        fields = reader.fieldnames or []
        if "task_id" not in fields or "label" not in fields:
            sys.exit("error: gold CSV 需要表头 task_id,label")
        for row in reader:
            tid = (row.get("task_id") or "").strip()
            lab = (row.get("label") or "").strip().lower()
            if tid and lab:
                mapping[tid] = lab
    return mapping


def _gold_for_task(task, label):
    """把 CSV label 转成任务 gold：单问题任务 yes/no（noul）或
    same/related/different（choice）；多问题任务（uc3/uc3s）用 c1,c3 选中列表。"""
    qs = task["questions"]
    if set(qs) == {"q"}:
        if qs["q"]["type"] == "choice":
            if label not in ("same", "related", "different"):
                raise ValueError(
                    f"choice 任务只接受 same/related/different，得到 {label!r}")
            return {"q": label}
        if label in ("yes", "true"):
            return {"q": True}
        if label in ("no", "false"):
            return {"q": False}
        raise ValueError(f"noul 任务只接受 yes/no，得到 {label!r}")
    if label.strip().lower() in ("none", "empty"):
        return {qid: False for qid in qs}
    selected = {s.strip() for s in label.split(",") if s.strip()}
    if selected and all(re.fullmatch(r"c\d+", s) for s in selected):
        unknown = selected - set(qs)
        if unknown:
            raise ValueError(f"选中的问题 id 不存在: {sorted(unknown)}")
        return {qid: qid in selected for qid in qs}
    raise ValueError(f"多问题任务接受形如 c1,c3 的选中列表，得到 {label!r}")


def apply_gold(tasks_by_suite, mapping):
    """任务构建后按 id 覆盖 gold。返回 (注入数, 未匹配 id 列表)。"""
    idx = {t["id"]: t for tasks in tasks_by_suite.values() for t in tasks}
    applied, bad = 0, []
    for tid, lab in mapping.items():
        t = idx.get(tid)
        if t is None:
            bad.append(tid)
            continue
        try:
            t["gold"] = _gold_for_task(t, lab)
            applied += 1
        except ValueError as exc:
            print(f"[warn] gold 注入跳过 {tid}: {exc}", file=sys.stderr)
    return applied, bad


def _stratified_by_sim(tasks, n, seed):
    """按 meta.sim 等频分层抽样 n 条；无 sim 字段的套件退化为随机抽样。"""
    rng = random.Random(seed)
    if len(tasks) <= n:
        return list(tasks)
    if any(not isinstance(t["meta"].get("sim"), (int, float)) for t in tasks):
        return rng.sample(tasks, n)
    srt = sorted(tasks, key=lambda t: t["meta"]["sim"])
    k = min(5, len(srt))
    strata = [srt[i * len(srt) // k:(i + 1) * len(srt) // k] for i in range(k)]
    quotas = [n * len(s) // len(srt) for s in strata]
    # 余数按小数部分从大到小补给各层
    order = sorted(range(k),
                   key=lambda i: -(n * len(strata[i]) / len(srt) - quotas[i]))
    for i in order:
        if sum(quotas) >= n:
            break
        quotas[i] += 1
    out = []
    for s, q in zip(strata, quotas):
        out.extend(rng.sample(s, min(q, len(s))))
    return out


def dump_unlabeled(fixtures_dir, suite, n, seed, out_path, gold_map=None):
    """把 suite 中 gold=None 的任务按 meta.sim 分层抽样 N 条，导出待标注 CSV
    （task_id,label 留空 + 供人读的 pair_desc / window_excerpt 摘要列）。"""
    tasks = build_suite(suite, fixtures_dir, seed=seed)
    if gold_map:
        apply_gold({suite: tasks}, gold_map)
    todo = [t for t in tasks if not t.get("gold")]
    picked = _stratified_by_sim(todo, n, seed)
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["task_id", "label", "pair_desc", "window_excerpt"])
        for t in picked:
            pair_desc, excerpt = "", ""
            if t["suite"] in ("uc3", "uc3s", "uc4"):
                excerpt = t["state"][:160].replace("\n", " ⏎ ")
            else:
                names = re.findall(
                    r"<(?:entity|node)_name>(.*?)</(?:entity|node)_name>",
                    t["state"], re.S)
                descs = re.findall(
                    r"<(?:entity|node)_description>(.*?)</(?:entity|node)_description>",
                    t["state"], re.S)
                parts = []
                for i, nm in enumerate(names):
                    d = descs[i].strip()[:100] if i < len(descs) else ""
                    parts.append(f"{nm.strip()}（{d}）" if d else nm.strip())
                pair_desc = (" <> ".join(parts)
                             or t["state"][:120].replace("\n", " "))
            w.writerow([t["id"], "", pair_desc, excerpt])
    return len(todo), len(picked)


def main():
    p = argparse.ArgumentParser(
        description="jev-memory benchmark driver (OpenRouter decisions endpoint).")
    p.add_argument("--fixtures", required=True,
                   help="fixtures 目录（uc1_pairs/uc2_pairs/uc3_windows/edges.jsonl）")
    p.add_argument("--api-key", default=os.environ.get("OPENROUTER_API_KEY"))
    p.add_argument("--model", default=DEFAULT_MODEL)
    p.add_argument("--suites", default="all",
                   help=f"'all'，或逗号分隔的套件/组。组: {list(SUITE_GROUPS)}。"
                        f"套件: {list(SUITE_BUILDERS)}。")
    p.add_argument("--limit", type=int, default=None, help="每套件任务数上限（冒烟用）")
    p.add_argument("--mock", action="store_true", help="离线合成答案，不走网络")
    p.add_argument("--mock-skill", type=float, default=0.9)
    p.add_argument("--out", default=None, help="输出目录（默认 runs/<时间戳>/）")
    p.add_argument("--concurrency", type=int, default=8)
    p.add_argument("--seed", type=int, default=0)
    p.add_argument("--skip-reference", action="store_true",
                   help="跳过 purpose 模型参照判定")
    p.add_argument("--gold", default=None,
                   help="人工金标 CSV（表头 task_id,label；label ∈ yes/no 或 "
                        "same/related/different；多问题套件用 c1,c3 选中列表）")
    p.add_argument("--dump-unlabeled", default=None, metavar="SUITE,N",
                   help="把指定 suite 里 gold=None 的任务按 meta.sim 分层抽样 N 条，"
                        "导出待标注 CSV 后退出")
    args = p.parse_args()

    try:
        fixtures_dir = check_fixtures_dir(args.fixtures)
    except FixturesMissing as exc:
        sys.exit(f"error: {exc}")

    suites = resolve_suites(args.suites)
    unknown = [s for s in suites if s not in SUITE_BUILDERS]
    if unknown:
        sys.exit(f"error: unknown suites: {unknown}")

    # --dump-unlabeled SUITE,N：导出待标注 CSV 后退出（不跑后端）
    if args.dump_unlabeled:
        try:
            dump_suite, dump_n = args.dump_unlabeled.rsplit(",", 1)
            dump_n = int(dump_n)
        except ValueError:
            sys.exit("error: --dump-unlabeled 需要 SUITE,N 形式")
        if dump_suite not in SUITE_BUILDERS:
            sys.exit(f"error: unknown suite: {dump_suite}")
        gold_map = load_gold_csv(args.gold) if args.gold else None
        if args.out:
            Path(args.out).mkdir(parents=True, exist_ok=True)
            dump_path = Path(args.out) / f"dump_unlabeled_{dump_suite}.csv"
        else:
            dump_path = Path(f"dump_unlabeled_{dump_suite}.csv")
        n_todo, n_picked = dump_unlabeled(
            fixtures_dir, dump_suite, dump_n, args.seed, dump_path, gold_map)
        print(f"[dump] {dump_suite}: 未标注 {n_todo} 条，导出 {n_picked} 条 → {dump_path}")
        return

    # reference judge（可选；mock 模式无 API key 环境，自动跳过）
    judge = None
    if not args.mock and not args.skip_reference:
        if not args.api_key:
            print("[warn] 无 OPENROUTER_API_KEY，跳过 reference 判定", file=sys.stderr)
        else:
            from reference import ReferenceJudge
            judge = ReferenceJudge(
                api_key=args.api_key,
                cache_path=fixtures_dir / "reference_cache.json",
            )

    if args.mock:
        backend = MockBackend(seed=args.seed, skill=args.mock_skill)
    else:
        if not args.api_key:
            sys.exit("error: pass --api-key or set the OPENROUTER_API_KEY environment variable")
        backend = OpenRouterBackend(
            api_key=args.api_key, model=args.model, title="jev-memory-bench")

    translator = judge.translate_state if judge else None

    # 任务构建（全部套件）→ 人工金标按 id 覆盖 → 执行
    tasks_by_suite = {
        name: build_suite(name, fixtures_dir, seed=args.seed, translator=translator)
        for name in suites
    }
    if args.gold:
        mapping = load_gold_csv(args.gold)
        applied, bad = apply_gold(tasks_by_suite, mapping)
        print(f"[gold] 注入 {applied} 条人工金标（CSV 共 {len(mapping)} 条）")
        if bad:
            print(f"[warn] {len(bad)} 个 task_id 未匹配: "
                  f"{bad[:5]}{'...' if len(bad) > 5 else ''}", file=sys.stderr)

    report = {"suites": {}, "started_at": time.strftime("%Y-%m-%dT%H:%M:%S%z")}
    all_rows = []
    rows_by_suite = {}
    for name in suites:
        tasks = tasks_by_suite[name]
        if args.limit:
            tasks = tasks[: args.limit]
        print(f"[run] {name}: {len(tasks)} requests")
        rows = run_tasks(tasks, backend, concurrency=args.concurrency)
        if judge is not None:
            attach_reference(name, tasks, rows, judge,
                             concurrency=args.concurrency)
        rows_by_suite[name] = rows
        all_rows.extend(rows)
        report["suites"][name] = aggregate(name, rows)
        print(f"[done] {name} (errors: {report['suites'][name]['n_errors']})")

    # UC-5：UC-1/3 中文校准聚合。有金标严格只用金标任务（参照单独出），
    # 无金标维持现状（参照标签，皆无则概率分位数）。
    pool_rows = [r for name in UC5_POOL for r in rows_by_suite.get(name, [])]
    if pool_rows:
        if any(r.get("gold") for r in pool_rows):
            gold_rows = [r for r in pool_rows if r.get("gold")]
            block = _calibration_block(
                _noul_points(gold_rows, lambda r: r["gold"]))
            ref_pts = _noul_points(pool_rows, _ref_labels)
            if any(g is not None for _, g in ref_pts):
                block["reference_metrics"] = _calibration_block(ref_pts)
            report["uc5_zh_calibration"] = block
        else:
            report["uc5_zh_calibration"] = _calibration_block(
                _noul_points(pool_rows))

    align = zh_en_alignment(rows_by_suite)
    if align:
        report["zh_en_alignment"] = align

    report["config"] = {
        "model": "mock" if args.mock else args.model,
        "suites": suites,
        "seed": args.seed,
        "limit": args.limit,
        "mock": args.mock,
        "reference": judge is not None,
    }
    out_dir = args.out or f"runs/{time.strftime('%Y%m%d_%H%M%S')}"
    out = write_outputs(out_dir, report, all_rows)
    print("\n== summary ==")
    print_summary(report)
    print(f"\nreport: {out / 'report.json'}\nraw:    {out / 'raw.jsonl'}")


if __name__ == "__main__":
    main()
