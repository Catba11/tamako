"""Metric functions. Pure stdlib; every function returns None on undefined input
(e.g. AUROC with a single class) instead of raising, so partial suites still
produce a report.

Ported from jev-demo jev_bench/metrics.py, 2026-09-18.
"""

from __future__ import annotations

import math
from collections import Counter


def accuracy(preds, golds):
    pairs = [(p, g) for p, g in zip(preds, golds) if p is not None and g is not None]
    if not pairs:
        return None
    return sum(p == g for p, g in pairs) / len(pairs)


def macro_f1(preds, golds):
    pairs = [(p, g) for p, g in zip(preds, golds) if p is not None and g is not None]
    if not pairs:
        return None
    labels = sorted({g for _, g in pairs} | {p for p, _ in pairs})
    f1s = []
    for lab in labels:
        tp = sum(p == lab and g == lab for p, g in pairs)
        fp = sum(p == lab and g != lab for p, g in pairs)
        fn = sum(p != lab and g == lab for p, g in pairs)
        if tp == 0:
            f1s.append(0.0)
            continue
        prec = tp / (tp + fp)
        rec = tp / (tp + fn)
        f1s.append(2 * prec * rec / (prec + rec))
    return sum(f1s) / len(f1s) if f1s else None


def mae(preds, golds):
    pairs = [(p, g) for p, g in zip(preds, golds) if p is not None and g is not None]
    if not pairs:
        return None
    return sum(abs(p - g) for p, g in pairs) / len(pairs)


def within_k(preds, golds, k=1.0):
    pairs = [(p, g) for p, g in zip(preds, golds) if p is not None and g is not None]
    if not pairs:
        return None
    return sum(abs(p - g) <= k for p, g in pairs) / len(pairs)


def _ranks(xs):
    order = sorted(range(len(xs)), key=lambda i: xs[i])
    ranks = [0.0] * len(xs)
    i = 0
    while i < len(order):
        j = i
        while j + 1 < len(order) and xs[order[j + 1]] == xs[order[i]]:
            j += 1
        avg = (i + j) / 2.0 + 1.0
        for k in range(i, j + 1):
            ranks[order[k]] = avg
        i = j + 1
    return ranks


def spearman(preds, golds):
    pairs = [(p, g) for p, g in zip(preds, golds) if p is not None and g is not None]
    if len(pairs) < 3:
        return None
    rp = _ranks([p for p, _ in pairs])
    rg = _ranks([g for _, g in pairs])
    n = len(rp)
    mp = sum(rp) / n
    mg = sum(rg) / n
    cov = sum((a - mp) * (b - mg) for a, b in zip(rp, rg))
    vp = sum((a - mp) ** 2 for a in rp)
    vg = sum((b - mg) ** 2 for b in rg)
    if vp == 0 or vg == 0:
        return None
    return cov / math.sqrt(vp * vg)


def brier_binary(probs, outcomes):
    pairs = [(p, o) for p, o in zip(probs, outcomes) if p is not None and o is not None]
    if not pairs:
        return None
    return sum((p - o) ** 2 for p, o in pairs) / len(pairs)


def log_loss_binary(probs, outcomes, eps=1e-7):
    pairs = [(p, o) for p, o in zip(probs, outcomes) if p is not None and o is not None]
    if not pairs:
        return None
    total = 0.0
    for p, o in pairs:
        p = min(max(p, eps), 1 - eps)
        total += -(o * math.log(p) + (1 - o) * math.log(1 - p))
    return total / len(pairs)


def reliability_bins(probs, outcomes, n_bins=10):
    """Equal-width bins -> list of {lo, hi, n, mean_pred, empirical}."""
    pairs = [(p, o) for p, o in zip(probs, outcomes) if p is not None and o is not None]
    bins = [[] for _ in range(n_bins)]
    for p, o in pairs:
        i = min(int(p * n_bins), n_bins - 1)
        bins[i].append((p, o))
    out = []
    for i, b in enumerate(bins):
        if not b:
            continue
        out.append(
            {
                "lo": i / n_bins,
                "hi": (i + 1) / n_bins,
                "n": len(b),
                "mean_pred": sum(p for p, _ in b) / len(b),
                "empirical": sum(o for _, o in b) / len(b),
            }
        )
    return out


def ece_binary(probs, outcomes, n_bins=10):
    pairs = [(p, o) for p, o in zip(probs, outcomes) if p is not None and o is not None]
    if not pairs:
        return None
    total = len(pairs)
    e = 0.0
    for b in reliability_bins(probs, outcomes, n_bins):
        e += (b["n"] / total) * abs(b["mean_pred"] - b["empirical"])
    return e


def ece_multiclass(confidences, corrects, n_bins=10):
    """Top-label calibration: confidence of the chosen option vs correctness."""
    return ece_binary(confidences, [float(c) for c in corrects], n_bins)


def auroc(scores, outcomes):
    """Rank-based AUROC; `scores` is P(yes)."""
    pairs = [(s, o) for s, o in zip(scores, outcomes) if s is not None and o is not None]
    pos = [s for s, o in pairs if o == 1]
    neg = [s for s, o in pairs if o == 0]
    if not pos or not neg:
        return None
    wins = 0.0
    for ps in pos:
        for ns in neg:
            if ps > ns:
                wins += 1.0
            elif ps == ns:
                wins += 0.5
    return wins / (len(pos) * len(neg))


def risk_coverage_curve(confidences, corrects, steps=21):
    """Selective prediction: sort by confidence desc, report accuracy at coverages.

    Returns {"curve": [{coverage, accuracy}], "aurc": normalized area under the
    risk-coverage curve (lower is better), "acc_at_50/80/100"}.
    """
    pairs = [(c, k) for c, k in zip(confidences, corrects) if c is not None and k is not None]
    if not pairs:
        return None
    pairs.sort(key=lambda x: -x[0])
    n = len(pairs)
    curve = []
    for i in range(steps):
        cov = i / (steps - 1)
        m = max(1, round(n * cov))
        acc = sum(k for _, k in pairs[:m]) / m
        curve.append({"coverage": cov, "accuracy": acc})
    aurc = 0.0
    for i in range(1, steps):
        c0, c1 = curve[i - 1]["coverage"], curve[i]["coverage"]
        r0 = 1 - curve[i - 1]["accuracy"]
        r1 = 1 - curve[i]["accuracy"]
        aurc += (c1 - c0) * (r0 + r1) / 2
    def acc_at(c):
        m = max(1, round(n * c))
        return sum(k for _, k in pairs[:m]) / m
    return {
        "curve": curve,
        "aurc": aurc,
        "acc_at_50": acc_at(0.5),
        "acc_at_80": acc_at(0.8),
        "acc_at_100": acc_at(1.0),
    }


def flip_rate(preds_per_group):
    """preds_per_group: list of lists of predictions for the same underlying item.
    Flip rate = fraction of groups whose predictions are not all identical."""
    if not preds_per_group:
        return None
    flips = sum(len(set(map(str, g))) > 1 for g in preds_per_group if g)
    return flips / len(preds_per_group)


def mean_pairwise_dev(values_per_group):
    """Mean |a-b| over all pairs within each group, averaged over groups."""
    devs = []
    for g in values_per_group:
        g = [v for v in g if v is not None]
        if len(g) < 2:
            continue
        tot = 0.0
        cnt = 0
        for i in range(len(g)):
            for j in range(i + 1, len(g)):
                tot += abs(g[i] - g[j])
                cnt += 1
        if cnt:
            devs.append(tot / cnt)
    return sum(devs) / len(devs) if devs else None


def latency_stats(latencies):
    xs = sorted(x for x in latencies if x is not None)
    if not xs:
        return None
    def pct(p):
        i = min(int(p * len(xs)), len(xs) - 1)
        return xs[i]
    return {
        "n": len(xs),
        "mean": sum(xs) / len(xs),
        "p50": pct(0.5),
        "p95": pct(0.95),
        "min": xs[0],
        "max": xs[-1],
    }


def label_distribution(golds):
    return dict(Counter(map(str, golds)))
