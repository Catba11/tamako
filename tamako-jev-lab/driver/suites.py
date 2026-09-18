"""Suite builders: fixtures dir -> task lists.

Task format follows jev-demo (Ported from jev-demo jev_bench/datasets.py task
shape, 2026-09-18):

    {"id": str, "suite": str, "state": str,
     "questions": {qid: {"type": "noul"|"choice", "instructions": str,
                         "criteria": {label: description}}},
     "gold": {qid: label} | None,
     "meta": {...}}

Fixture files (produced by the upstream Rust extractor; read-only here):

  uc1_pairs.jsonl   {"chat_id","a_id","b_id","a_name","a_desc","b_name",
                     "b_desc","sim":float,"kind":"person"|"concept"}
                     -- same-kind node pairs with sim in [0.75, 1.0)
  uc2_pairs.jsonl   same fields (sim >= 0.90 subset, merge replication)
  uc3_windows.jsonl {"chat_id","window_id","prompt_text":str,
                     "candidates":[{"edge_id","edge_text","valid_at"}],
                     "injected_edge_ids":[str]}
  edges.jsonl       {"chat_id","edge_id","source_name","target_name",
                     "relationship","edge_text","valid_at","invalid_at"}

Suites:
  uc1a   sim >= 0.88 pairs, noul same-entity confirmation, gold=None (labels
         filled later by reference.py)
  uc1b   sim in [0.75, 0.88) random sample <= 300 (seed 0), same shape
  uc1s   synthetic labeled pairs seeded from real uc1 nodes: same (rule
         transforms: nickname/abbrev/translation/paraphrase), hard-different
         (same-kind same-chat cross-pair nodes), easy-different (cross-chat
         random pairs); <= 100 per category, gold known by construction
  uc2    merge-verdict choice task over uc2_pairs (same/related/different)
  uc3    per-window per-candidate noul "would omitting this candidate
         materially reduce reply quality?"
  uc3s   synthetic windows: a real edge buried among <= 40 real distractor
         edges, template-generated messages (direct-question / topic-
         continuation / unrelated-chitchat), gold = buried index (chitchat:
         nothing worth injecting)
  uc4    per-window single noul "is any candidate worth injecting?"
  zh_en  30 pairs sampled from uc1a (seed 0), state translated to English via
         reference.py's translate cache (identity fallback when offline)
"""

from __future__ import annotations

import json
import random
import re
import sys
from pathlib import Path

UC1A_SIM_MIN = 0.88
UC1B_SAMPLE_MAX = 300
UC1S_PER_CATEGORY_MAX = 100
UC3S_WINDOW_MAX = 100
UC3S_CANDIDATE_MAX = 40
ZH_EN_SAMPLE = 30

FIXTURE_FILES = {
    "uc1a": "uc1_pairs.jsonl",
    "uc1b": "uc1_pairs.jsonl",
    "uc1s": "uc1_pairs.jsonl",
    "uc2": "uc2_pairs.jsonl",
    "uc3": "uc3_windows.jsonl",
    "uc3s": "edges.jsonl",
    "uc4": "uc3_windows.jsonl",
    "zh_en": "uc1_pairs.jsonl",
}

SUITE_BUILDERS = {}
SUITE_GROUPS = {
    "uc1": ["uc1a", "uc1b", "uc1s"],
    "recall": ["uc3", "uc3s", "uc4"],
}


class FixturesMissing(Exception):
    """Raised when the fixtures directory itself does not exist."""


def check_fixtures_dir(fixtures_dir):
    d = Path(fixtures_dir)
    if not d.is_dir():
        raise FixturesMissing(
            f"fixtures 缺失: {d} —— 目录不存在。请先用上游 extractor 产出 "
            f"fixtures（uc1_pairs.jsonl / uc2_pairs.jsonl / uc3_windows.jsonl / edges.jsonl）"
        )
    return d


def load_jsonl(fixtures_dir, name):
    """Read a fixture file; a missing file yields an empty list with a warning
    (partial fixture dirs still run the suites that have data)."""
    path = Path(fixtures_dir) / name
    if not path.is_file():
        print(f"[warn] fixtures 文件缺失，按空套件处理: {path}", file=sys.stderr)
        return []
    rows = []
    with path.open(encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                rows.append(json.loads(line))
    return rows


# ---------------------------------------------------------------------------
# shared renders / question shapes
# ---------------------------------------------------------------------------

def pair_state(a_name, a_desc, b_name, b_desc):
    """The confirmation-prompt rendering (tamako resolve.rs shape)."""
    return (
        "Extracted entity:\n"
        f"<entity_name>{a_name}</entity_name>\n"
        f"<entity_description>{a_desc}</entity_description>\n\n"
        "Candidate graph node:\n"
        f"<node_name>{b_name}</node_name>\n"
        f"<node_description>{b_desc}</node_description>"
    )


def merge_state(a_name, a_desc, b_name, b_desc, kind):
    """The merge-confirmation rendering (tamako merge_confirm.rs shape)."""
    return (
        "Node A:\n"
        f"<node_name>{a_name}</node_name>\n"
        f"<node_kind>{kind}</node_kind>\n"
        f"<node_description>{a_desc}</node_description>\n\n"
        "Node B:\n"
        f"<node_name>{b_name}</node_name>\n"
        f"<node_kind>{kind}</node_kind>\n"
        f"<node_description>{b_desc}</node_description>"
    )


def confirm_question():
    return {
        "type": "noul",
        "instructions": (
            "判断「提取实体」与「候选图节点」两段描述是否指向同一个现实实体。"
            "表面形式可以不同：昵称、缩写、翻译都算同一个实体；存疑时答「否」"
            "（一次错误合并比保留一个重复节点更糟）。"
        ),
        "criteria": {
            "true": "两段描述明确指向同一个现实实体（昵称/缩写/翻译均算同一）",
            "false": "并非同一实体，或无法确定——存疑即选此项",
        },
    }


def merge_question():
    return {
        "type": "choice",
        "instructions": (
            "判断两个记忆图节点的关系。跨语言同义词算 related 而非 same；"
            "在 same 与 related 之间存疑时选 related（错误合并比重复节点更糟）。"
        ),
        "criteria": {
            "same": "同一现实实体：两个节点可安全合并为一个",
            "related": "相关但并非同一实体（含跨语言同义词）：保持两个独立节点，仅记录关联",
            "different": "无关：跳过该对",
        },
    }


def uc3_candidate_question(index, edge_text):
    return {
        "type": "noul",
        "instructions": (
            f"以上是群聊新消息。候选记忆 {index}：{edge_text}\n"
            f"若回复这些新消息时省略候选 {index}，会实质降低回复质量吗？"
            "拿不准就选「否」——什么都不选是常态，仅沾边不算数。"
        ),
        "criteria": {
            "true": "省略该候选会实质降低回复质量，值得注入",
            "false": "可有可无或无关，不应注入",
        },
    }


def uc4_question():
    return {
        "type": "noul",
        "instructions": (
            "以上是群聊新消息和候选记忆列表。这些候选中是否有任何一条值得注入"
            "（即省略它会实质降低回复质量）？拿不准就选「否」。"
        ),
        "criteria": {
            "true": "至少一条候选值得注入",
            "false": "没有任何候选值得注入",
        },
    }


def _pair_meta(p, band):
    return {
        "sim": p.get("sim"),
        "band": band,
        "chat_id": p.get("chat_id"),
        "kind": p.get("kind"),
        "pair": [p.get("a_id"), p.get("b_id")],
    }


# ---------------------------------------------------------------------------
# uc1a / uc1b / zh_en
# ---------------------------------------------------------------------------

def _uc1_task(suite, idx, p, band, state=None, extra_meta=None):
    meta = _pair_meta(p, band)
    if extra_meta:
        meta.update(extra_meta)
    return {
        "id": f"{suite}-{idx:04d}",
        "suite": suite,
        "state": state if state is not None else pair_state(
            p["a_name"], p["a_desc"], p["b_name"], p["b_desc"]
        ),
        "questions": {"q": confirm_question()},
        "gold": None,  # 标签后补（reference 判定）
        "meta": meta,
    }


def build_uc1a(fixtures_dir, seed=0, translator=None):
    pairs = [p for p in load_jsonl(fixtures_dir, "uc1_pairs.jsonl")
             if p.get("sim") is not None and p["sim"] >= UC1A_SIM_MIN]
    return [_uc1_task("uc1a", i, p, "high") for i, p in enumerate(pairs)]


def build_uc1b(fixtures_dir, seed=0, translator=None):
    pairs = [p for p in load_jsonl(fixtures_dir, "uc1_pairs.jsonl")
             if p.get("sim") is not None and 0.75 <= p["sim"] < UC1A_SIM_MIN]
    rng = random.Random(seed)
    if len(pairs) > UC1B_SAMPLE_MAX:
        pairs = rng.sample(pairs, UC1B_SAMPLE_MAX)
    return [_uc1_task("uc1b", i, p, "mid") for i, p in enumerate(pairs)]


def build_zh_en(fixtures_dir, seed=0, translator=None):
    """zh/en contrast: 30 uc1a pairs, state translated to English via the
    reference translate cache. Without a translator (offline/--mock) the
    Chinese state is reused and meta.translated is False."""
    pairs = [p for p in load_jsonl(fixtures_dir, "uc1_pairs.jsonl")
             if p.get("sim") is not None and p["sim"] >= UC1A_SIM_MIN]
    indexed = list(enumerate(pairs))  # (uc1a task index, pair)
    rng = random.Random(seed)
    if len(indexed) > ZH_EN_SAMPLE:
        indexed = rng.sample(indexed, ZH_EN_SAMPLE)
    tasks = []
    for j, (i, p) in enumerate(indexed):
        zh_state = pair_state(p["a_name"], p["a_desc"], p["b_name"], p["b_desc"])
        if translator is not None:
            en_state = translator(zh_state)
            translated = True
        else:
            en_state = zh_state
            translated = False
        tasks.append(_uc1_task(
            "zh_en", j, p, "high", state=en_state,
            extra_meta={"lang": "en", "zh_task_id": f"uc1a-{i:04d}",
                        "translated": translated},
        ))
    return tasks


# ---------------------------------------------------------------------------
# uc1s: synthetic labeled pairs seeded from real uc1 nodes
# ---------------------------------------------------------------------------

# 小型内置词表：规则化「中英互译」变换（无 LLM）。命不中词表时该变换跳过。
_ZH_EN_GLOSSARY = {
    "机器学习": "machine learning",
    "深度学习": "deep learning",
    "强化学习": "reinforcement learning",
    "大语言模型": "large language model",
    "人工智能": "artificial intelligence",
    "神经网络": "neural network",
    "自然语言处理": "natural language processing",
    "计算机视觉": "computer vision",
    "数据库": "database",
    "操作系统": "operating system",
    "编译器": "compiler",
    "算法": "algorithm",
    "开源": "open source",
    "程序员": "programmer",
    "工程师": "engineer",
}

_DESC_SYNONYMS = [
    ("喜欢", "偏爱"), ("经常", "常常"), ("很多", "大量"), ("研究", "钻研"),
    ("负责", "打理"), ("讨论", "探讨"), ("使用", "采用"), ("认为", "觉得"),
]

_HAS_CJK = re.compile(r"[一-鿿]")


def _nickify(name):
    """昵称化：中文名取「小+首字」，英文名取首词。"""
    if _HAS_CJK.search(name):
        return "小" + name[0] if len(name) >= 2 else name + name
    parts = name.split()
    return parts[0] if parts else name


def _abbreviate(name):
    """缩写：英文多词取首字母；中文长名取首尾字。无法缩写返回 None。"""
    words = re.findall(r"[A-Za-z]+", name)
    if len(words) >= 2:
        return "".join(w[0].upper() for w in words)
    if not words and len(name) >= 3:
        return name[0] + name[-1]
    return None


def _translate_name(name):
    """中英互译：命中内置词表才变换，否则 None（调用方换别的变换）。"""
    for zh, en in _ZH_EN_GLOSSARY.items():
        if zh in name:
            return name.replace(zh, en)
    low = name.lower()
    for zh, en in _ZH_EN_GLOSSARY.items():
        if en in low:
            return re.sub(re.escape(en), zh, name, flags=re.IGNORECASE)
    return None


def _paraphrase_desc(desc, rng):
    """描述换述：同义词替换 + 子句重排 + 模板包裹（纯字符串规则）。"""
    out = desc
    for src, dst in _DESC_SYNONYMS:
        if src in out:
            out = out.replace(src, dst, 1)
            break
    clauses = [c for c in re.split(r"(?<=[。；;])\s*", out) if c]
    if len(clauses) > 1:
        rng.shuffle(clauses)
        out = "".join(clauses)
    wrap = rng.choice(["据了解，{d}", "{d}（群里常提到）", "群里都知道：{d}"])
    return wrap.format(d=out)


def _same_variants(node, rng):
    """Yield (transform_name, new_name, new_desc) same-entity variants."""
    transforms = []
    nick = _nickify(node["name"])
    if nick != node["name"]:
        transforms.append(("nickname", nick))
    abbr = _abbreviate(node["name"])
    if abbr and abbr != node["name"]:
        transforms.append(("abbrev", abbr))
    trans = _translate_name(node["name"])
    if trans and trans != node["name"]:
        transforms.append(("translate", trans))
    transforms.append(("paraphrase", node["name"]))  # 名不变、描述换述
    rng.shuffle(transforms)
    for tname, new_name in transforms:
        new_desc = _paraphrase_desc(node["desc"], rng) if tname == "paraphrase" else node["desc"]
        if new_name != node["name"] or new_desc != node["desc"]:
            yield tname, new_name, new_desc


def _seed_nodes(pairs):
    """Unique real nodes from uc1_pairs (fixtures have no nodes.jsonl)."""
    nodes = {}
    for p in pairs:
        for side in ("a", "b"):
            nid = p.get(f"{side}_id")
            if nid is None:
                continue
            nodes.setdefault(nid, {
                "id": nid,
                "name": p.get(f"{side}_name", ""),
                "desc": p.get(f"{side}_desc", ""),
                "kind": p.get("kind"),
                "chat_id": p.get("chat_id"),
            })
    return nodes


def _uc1s_task(idx, a, b, gold_same, category, transform=None):
    meta = {
        "synthetic": True,
        "category": category,
        "chat_id": a.get("chat_id"),
        "kind": a.get("kind"),
        "pair": [a.get("id"), b.get("id")],
    }
    if transform:
        meta["transform"] = transform
    if b.get("chat_id") != a.get("chat_id"):
        meta["b_chat_id"] = b.get("chat_id")
    return {
        "id": f"uc1s-{idx:04d}",
        "suite": "uc1s",
        "state": pair_state(a["name"], a["desc"], b["name"], b["desc"]),
        "questions": {"q": confirm_question()},
        "gold": {"q": gold_same},
        "meta": meta,
    }


def build_uc1s(fixtures_dir, seed=0, translator=None):
    """Synthetic labeled pairs: same via rule transforms of real nodes;
    hard-different = same-kind same-chat nodes from distinct real pairs;
    easy-different = cross-chat random pairs. <= 100 per category, seed 0."""
    pairs = load_jsonl(fixtures_dir, "uc1_pairs.jsonl")
    rng = random.Random(seed)
    nodes = _seed_nodes(pairs)
    tasks = []

    # -- same -----------------------------------------------------------
    seeds = list(nodes.values())
    rng.shuffle(seeds)
    n_same = 0
    for node in seeds:
        if n_same >= UC1S_PER_CATEGORY_MAX:
            break
        for tname, new_name, new_desc in _same_variants(node, rng):
            variant = dict(node, name=new_name, desc=new_desc,
                           id=f"{node['id']}#syn-{tname}")
            tasks.append(_uc1s_task(len(tasks), node, variant, True, "same", tname))
            n_same += 1
            break

    # -- hard different ---------------------------------------------------
    # 真实难负例本该取 sim∈[0.60,0.75) 的同 kind 对，但 uc1_pairs 只含
    # sim≥0.75；改用「同群同 kind、来自不同真实对」的节点配对作近似难负例。
    real_pairs = {tuple(sorted((p.get("a_id"), p.get("b_id")))) for p in pairs}
    by_cell = {}
    for n in nodes.values():
        by_cell.setdefault((n["chat_id"], n["kind"]), []).append(n)
    hard_candidates = []
    for cell_nodes in by_cell.values():
        for i in range(len(cell_nodes)):
            for j in range(i + 1, len(cell_nodes)):
                key = tuple(sorted((cell_nodes[i]["id"], cell_nodes[j]["id"])))
                if key not in real_pairs:
                    hard_candidates.append((cell_nodes[i], cell_nodes[j]))
    rng.shuffle(hard_candidates)
    for a, b in hard_candidates[:UC1S_PER_CATEGORY_MAX]:
        tasks.append(_uc1s_task(len(tasks), a, b, False, "hard_different"))

    # -- easy different ---------------------------------------------------
    all_nodes = list(nodes.values())
    n_easy = 0
    attempts = 0
    while n_easy < UC1S_PER_CATEGORY_MAX and len(all_nodes) >= 2 and attempts < 5000:
        attempts += 1
        a, b = rng.sample(all_nodes, 2)
        if a["chat_id"] == b["chat_id"]:
            continue  # 跨群配对保证「明显不同」
        tasks.append(_uc1s_task(len(tasks), a, b, False, "easy_different"))
        n_easy += 1

    return tasks


# ---------------------------------------------------------------------------
# uc2: merge-verdict choice
# ---------------------------------------------------------------------------

def build_uc2(fixtures_dir, seed=0, translator=None):
    pairs = load_jsonl(fixtures_dir, "uc2_pairs.jsonl")
    tasks = []
    for i, p in enumerate(pairs):
        tasks.append({
            "id": f"uc2-{i:04d}",
            "suite": "uc2",
            "state": merge_state(p["a_name"], p["a_desc"], p["b_name"], p["b_desc"],
                                 p.get("kind", "concept")),
            "questions": {"q": merge_question()},
            "gold": None,  # 标签后补（reference 判定）
            "meta": _pair_meta(p, "merge"),
        })
    return tasks


# ---------------------------------------------------------------------------
# uc3 / uc4: real windows
# ---------------------------------------------------------------------------

def _uc3_questions(candidates):
    return {
        f"c{i + 1}": uc3_candidate_question(i + 1, c.get("edge_text", ""))
        for i, c in enumerate(candidates)
    }


def build_uc3(fixtures_dir, seed=0, translator=None):
    windows = load_jsonl(fixtures_dir, "uc3_windows.jsonl")
    tasks = []
    for w in windows:
        cands = w.get("candidates", [])
        tasks.append({
            "id": f"uc3-{w.get('chat_id')}-{w.get('window_id')}",
            "suite": "uc3",
            "state": w.get("prompt_text", ""),
            "questions": _uc3_questions(cands),
            "gold": None,  # 标签后补（reference gate 判定）
            "meta": {
                "chat_id": w.get("chat_id"),
                "window_id": w.get("window_id"),
                "n_candidates": len(cands),
                "candidate_texts": [c.get("edge_text", "") for c in cands],
                "candidate_edge_ids": [c.get("edge_id") for c in cands],
                "injected_edge_ids": w.get("injected_edge_ids", []),
            },
        })
    return tasks


def build_uc4(fixtures_dir, seed=0, translator=None):
    windows = load_jsonl(fixtures_dir, "uc3_windows.jsonl")
    tasks = []
    for w in windows:
        cands = w.get("candidates", [])
        listing = "\n".join(
            f"{i + 1}. {c.get('edge_text', '')}" for i, c in enumerate(cands)
        )
        state = f"{w.get('prompt_text', '')}\n\n候选记忆：\n{listing}"
        tasks.append({
            "id": f"uc4-{w.get('chat_id')}-{w.get('window_id')}",
            "suite": "uc4",
            "state": state,
            "questions": {"q": uc4_question()},
            "gold": None,  # 标签后补（reference gate 判定）
            "meta": {
                "chat_id": w.get("chat_id"),
                "window_id": w.get("window_id"),
                "n_candidates": len(cands),
                "candidate_edge_ids": [c.get("edge_id") for c in cands],
                "candidate_texts": [c.get("edge_text", "") for c in cands],
                "prompt_text": w.get("prompt_text", ""),
                "injected_edge_ids": w.get("injected_edge_ids", []),
            },
        })
    return tasks


# ---------------------------------------------------------------------------
# uc3s: synthetic windows with a buried real edge
# ---------------------------------------------------------------------------

# 中文模板（字符串模板，无 LLM）。{source}/{target}/{rel} 来自真实边字段。
_UC3S_DIRECT_TEMPLATES = [
    "A: 哎，{source} 和 {target} 到底是什么关系来着？\nB: 我也记不清了，谁能说说？",
    "A: 问个事，关于{target}，{source} 之前是不是提过什么？\nB: 好像有印象。",
    "A: 谁知道 {source} 的「{rel}」指的是哪一位？\nB: 同问。",
]
_UC3S_TOPIC_TEMPLATES = [
    "A: 最近又聊到{target}了。\nB: 是啊，{source} 那边好像一直有动静。",
    "A: 说到{target}我就想到{source}。\nB: 哈哈确实，老话题了。",
    "A: {source} 刚才那个话题挺有意思的。\nB: 跟{target}也有点关系吧。",
]
_UC3S_CHITCHAT_TEMPLATES = [
    "A: 今晚吃什么？\nB: 随便，点个外卖吧。",
    "A: 哈哈哈这个表情包绝了。\nB: 发我发我。",
    "A: 周末有人打球吗？\nB: 看天气吧。",
]


def build_uc3s(fixtures_dir, seed=0, translator=None):
    """Synthetic recall windows: one real edge buried among <= 40 real
    distractor edges (same chat preferred). gold = buried candidate for the
    direct/topic templates; the unrelated-chitchat template expects NOTHING
    to be injected (gate conservatism check)."""
    edges = load_jsonl(fixtures_dir, "edges.jsonl")
    rng = random.Random(seed)
    if not edges:
        return []
    by_chat = {}
    for e in edges:
        by_chat.setdefault(e.get("chat_id"), []).append(e)

    buried_pool = list(edges)
    rng.shuffle(buried_pool)
    template_cycle = (["direct"] * 2 + ["topic"] * 2 + ["chitchat"])  # 2:2:1
    tasks = []
    for w_idx, buried in enumerate(buried_pool[:UC3S_WINDOW_MAX]):
        kind = template_cycle[w_idx % len(template_cycle)]
        fmt = {
            "source": buried.get("source_name", ""),
            "target": buried.get("target_name", ""),
            "rel": buried.get("relationship", ""),
        }
        if kind == "direct":
            body = rng.choice(_UC3S_DIRECT_TEMPLATES).format(**fmt)
        elif kind == "topic":
            body = rng.choice(_UC3S_TOPIC_TEMPLATES).format(**fmt)
        else:
            body = rng.choice(_UC3S_CHITCHAT_TEMPLATES)
        prompt_text = f"群聊新消息：\n{body}"

        chat_edges = [e for e in by_chat.get(buried.get("chat_id"), [])
                      if e.get("edge_id") != buried.get("edge_id")]
        other_edges = [e for e in edges
                       if e.get("chat_id") != buried.get("chat_id")
                       and e.get("edge_id") != buried.get("edge_id")]
        rng.shuffle(chat_edges)
        rng.shuffle(other_edges)
        distractors = (chat_edges + other_edges)[:UC3S_CANDIDATE_MAX - 1]
        candidates = [buried] + distractors
        rng.shuffle(candidates)
        buried_idx = next(i for i, c in enumerate(candidates)
                          if c.get("edge_id") == buried.get("edge_id"))

        expect_inject = kind in ("direct", "topic")
        gold = {f"c{i + 1}": (i == buried_idx and expect_inject)
                for i in range(len(candidates))}
        tasks.append({
            "id": f"uc3s-{w_idx:04d}",
            "suite": "uc3s",
            "state": prompt_text,
            "questions": _uc3_questions(candidates),
            "gold": gold,
            "meta": {
                "synthetic": True,
                "template": kind,
                "chat_id": buried.get("chat_id"),
                "n_candidates": len(candidates),
                "candidate_edge_ids": [c.get("edge_id") for c in candidates],
                "candidate_texts": [c.get("edge_text", "") for c in candidates],
                "buried_edge_id": buried.get("edge_id"),
                "expected_edge_ids": [buried.get("edge_id")] if expect_inject else [],
            },
        })
    return tasks


# ---------------------------------------------------------------------------

SUITE_BUILDERS.update({
    "uc1a": build_uc1a,
    "uc1b": build_uc1b,
    "uc1s": build_uc1s,
    "uc2": build_uc2,
    "uc3": build_uc3,
    "uc3s": build_uc3s,
    "uc4": build_uc4,
    "zh_en": build_zh_en,
})


def build_suite(name, fixtures_dir, seed=0, translator=None):
    return SUITE_BUILDERS[name](fixtures_dir, seed=seed, translator=translator)
