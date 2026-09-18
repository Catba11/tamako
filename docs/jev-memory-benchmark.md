# Jev × Tamako Memory：Benchmark 方案

> 分支 `jev-memory` 交付物 1/2。目标：评估 TypeSafe Jev（决策型 LLM，只输出
> choice/score/noul 结构化判断，不生成文本）能否替代/增强 tamako memory
> 子系统中现有的生成式 LLM 判断点。**全程离线实验**：数据为 2026-09-12
> drain 时刻的 Mac 数据根快照副本（桌面 `~/jev-lab/data/`，8 群），不触碰
> 任何生产环境。Jev 用法与实测特性见 jev-demo 仓库 `jev_guidebook.md`
> （`typesafe/jev-1.13` @ OpenRouter alpha decisions 端点，$0.042/Mtok
> 输入，输出免费）。

## 1. 为什么 Jev 可能适合 tamako memory

tamako 的记忆管线里有三个「判断」环节目前由生成式 LLM（claude-haiku-4-5）
以结构化 JSON 输出完成——它们本质都是**决策**，不是生成：

| # | 环节 | 现状实现 | 输出 |
|---|---|---|---|
| J1 | 实体消解确认（digest consolidation） | haiku，`ConfirmationAnswer{same,reason}`（resolve.rs:148-196，preamble :172-181） | 二元 same |
| J2 | merge 三方裁决（离线 consolidation 工具） | haiku，`MergeConfirmation{verdict,reason}`（merge_confirm.rs:52-65，preamble :92-96） | 三选 same/related/different |
| J3 | relevance gate（retrieval 注入筛选） | haiku，`RecallSelection{selected[],reason}`（recall.rs:413-431 preamble，:480-520 prompt） | ≤5 条候选的排序子集 |

Jev 的卖点与这三个环节的匹配点：结构化概率输出（可直接做阈值路由与
selective prediction）、校准优秀（jev-demo 实测 ECE 0.036–0.053）、
fan-out 便宜（一次请求最多 32+ 问题仅 1.75× 延迟）、单价极低。
**已知的最大风险**：guidebook 明示 CJK 负载准确率较低需在自有数据验证——
tamako 群聊几乎全中文，这正是本实验要回答的核心问题之一（UC-5）。

## 2. 数据资产盘点（快照副本实测）

| 群 | messages | edge_texts | injected_memories | merge_audit |
|---|---|---|---|---|
| -1001234567890 | 11 | 0 | 0 | 0 |
| g1 | 3,087 | 3,863 | 4 | 0 |
| g2 | 32,860 | 29,015 | 13 | 0 |
| g3 | 177 | 269 | 2 | 0 |
| g4 | 3,670 | 3,593 | 13 | 0 |
| g5 | 4,061 | 4,283 | 12 | 0 |
| g6 | 4,687 | 4,752 | 8 | 0 |
| g7 | 4,827 | 5,334 | 24 | 0 |
| **合计** | **57,300** | **51,109** | **76** | **0** |

标签资产结论：

- **merge_audit 全空**：无历史 merge 裁决标签 → UC-1/UC-2 的参照标签只能
  来自 (a) haiku 实时重判（参照，非真值），(b) 人工标注子集（金标）。
- **injected_memories 76 条正例**：生产真实注入记录（edge_id +
  injection_position + range_tag + created_at + content）。只有正例、无
  候选全集，不能直接算 precision/recall，但可作为 UC-3 重放的**命中
  校验点**（Jev/haiku 重放到同一时刻是否也会选中这些边）。
- **向量资产**：node_embeddings（vec0，float[3072]，cosine 距离，
  schema v11）覆盖全部实体节点；全量相似度可离线计算，无需重新 embedding。

## 3. 现状判断点精确规格（mock 的语义对齐基准）

### J1 实体消解确认（resolve.rs）
- 触发：digest 提取新实体 → embedding → KNN（overfetch 10，resolve.rs:52）
  → cosine sim ≥ **0.88**（config.rs:376 `vector_candidate_threshold`）的
  候选各一次确认；< 0.88 直接新建节点（**漏并带**就此产生）。
- 预算：`resolution_confirm_budget` = 5/digest（耗尽按低于阈值处理）。
- 语义规则（CONFIRMATION_PREAMBLE）：昵称/缩写/翻译 = 同一实体；存疑
  判 false（错并比重复节点更糟）。
- kind 兼容过滤先行（Person↔Person 等，resolve.rs:964-971）。

### J2 merge 三方裁决（merge_confirm.rs + merge.rs）
- 触发：离线工具，KNN(k=5) ≥ **0.90**（`merge_candidate_threshold`），
  已 known_as/also_known_as 连接的对排除，无序对去重。
- 三方语义：same=合并；related=记 related_pairs 表、**不建边**
  （decision 83）；different=跳过。

### J3 relevance gate（recall.rs）
- 候选组装：浅层入口 + 向量 KNN（k=5，sim≥0.88）+ 两跳扩展（≤500，
  90 天窗）+ edge_texts FTS（CJK n-gram n∈2..=5），总 cap 40。
- gate：haiku 一次性从 ≤40 候选中选 ≤5（`recall_injection_cap`），按
  相关度降序；失败=不注入（fail-closed）。
- 语义规则（RECALL_PREAMBLE rule 2/3）：「省略它会实质降低回复质量才选」；
  「存疑则不选，不选是常态」。
- prompt 形状：可选 context view 前缀 + 新消息 XML 行 + 编号候选列表
  `N. {edge_text} (since YYYY-MM-DD)`。

## 4. Use case 目录

### UC-1a：实体消解确认的 Noul 化（≥0.88 确认带）
- **Jev mock**：noul。instructions 逐字对齐 J1 规则（同一现实实体？昵称/
  缩写/翻译算同一；存疑答否）。state = entity/candidate 的 name+description
  （沿用 `<entity_name>` 等定界符格式）。
- **数据集**：离线计算快照内全部同-kind 节点对 cosine 相似度，取
  sim ∈ [0.88, 1.0) 的对（即生产中会触发 haiku 确认的对）。预计几十~
  几百对，全量跑。
- **参照/金标**：haiku 实时重判（同 CONFIRMATION_PREAMBLE）为参照；
  分层抽样 ≥60 对人工标注为金标。
- **指标**：accuracy/F1@0.5（对金标与对参照各一份）、AUROC、ECE/Brier、
  标定阈值下的 accuracy-coverage 曲线（AURC 思路，用 noul 概率自身）、
  与 haiku 一致率、单判成本/延迟、确定性（×3 重复 flip rate）。
- **价值假设**：noul 概率替代 0.88 一刀切 → 高概率自动并、中间带交
  haiku 复核、低概率新建（confidence-gated routing 模式）。

### UC-1b：漏并带回收（0.75–0.88 带）
- **Jev mock**：同 UC-1a 的 noul，跑 sim ∈ [0.75, 0.88) 的节点对
  （生产中直接新建、从不确认——潜在的重复节点来源）。
- **数据集**：随机抽样 ≤300 对（全量预计数千对）。
- **参照/金标**：Jev P(same) ≥ 0.8 的对交 haiku 复核 + 全部抽样人工
  标注。
- **指标**：带内真重复率（金标）、Jev 在该带的 AUROC/ECE、
  「若按 P≥τ 合并」在不同 τ 下的精度/召回曲线。
- **价值假设**：若带内真重复率可观且 Jev 能校准地挑出，则
  vector_candidate_threshold 可从硬阈值改为 Jev 筛 + haiku 兜底。

### UC-2：merge 三方裁决的 Choice 化
- **Jev mock**：choice{criteria: same/related/different}，描述逐字对齐
  J2 三方语义（related 不建边）。
- **数据集**：快照内 KNN(k=5 复刻) ≥0.90 的节点对（merge_audit 为空，
  无历史标签，全部现场判）。
- **参照/金标**：haiku merge confirmation 实时重判为参照；人工标注
  ≤80 对为金标。
- **指标**：macro-F1、per-class accuracy、same↔related 混淆率（最危险
  方向：related 误判 same 会错并）、ECE（multiclass confidence）、AURC、
  与 haiku 三方一致率、成本。
- **价值假设**：choice 的 confidence 可直接驱动「自动并 / 人工复核 /
  跳过」三档路由。

### UC-3：relevance gate 的 Noul fan-out 化
- **Jev mock**：state = 复刻 render_recall_prompt 的用户段（新消息 XML
  行 + 编号候选列表，无 context view——decision-72 前缀是 haiku 共享
  优化，Jev 实验先测裸形态）；questions = 每候选一个 noul「若回复这些
  新消息时省略候选 i，会实质降低回复质量吗？」（rule 2 字面化）。一次
  请求 ≤40 问题（fan-out 实测廉价）。选择 = P 降序 top-5。
- **数据集**：重放 wake 时刻。取样两类窗口：(a) injected_memories 的
  created_at 对齐的消息窗（76 条正例所属窗口，含真值命中校验）；
  (b) 随机活跃窗。合计 ≤60 窗。候选集离线复刻 recall.rs 组装（向量
  KNN + FTS + 两跳，同一 cap 40）。
- **参照/金标**：haiku gate 对同一 prompt 实时选择为参照；
  injected_memories 对齐窗做命中校验；≤20 窗人工标注相关集为金标。
- **指标**：set-level P/R/F1（对参照与金标）、P 排序 vs haiku 排序的
  Spearman、ECE、注入条数分布对比、单窗延迟/成本对比（fan-out 一次 vs
  haiku 一次）。
- **价值假设**：每候选独立概率 → 注入数自适应（不再固定 top-5 再 cap），
  且低概率候选可省掉 haiku 调用。

### UC-4：「是否注入任何记忆」前置 Noul
- **Jev mock**：单 noul「这些候选中是否有任何一条值得注入」（rule 3
  「不选是常态」的反面利用）。
- **数据集**：复用 UC-3 的窗口与候选。
- **参照**：haiku gate 空/非空结果。
- **指标**：accuracy、AUROC、ECE；运营成本估算 = 可跳过的 gate 调用比例
  × 误杀率（假阴性率）。
- **价值假设**：若校准良好，可作为 haiku gate 前的廉价熔断器。

### UC-5：中文负载校准与稳定性（横切）
- **内容**：聚合 UC-1/3 全部中文样本的 noul/choice 校准（ECE/Brier/
  可靠性分箱）；确定性（同请求 ×3 flip rate）；中英对照小集（30 对样本
  英文重写，比较概率偏移）——直接回应 guidebook §3.6 的 CJK 警示。
- **产出**：「Jev 能否用于中文群聊记忆判断」的实证结论 + 若可用的
  推荐阈值区间。

### 明确排除（附理由）
- **supersedure / single-value invalidation**：机械 last-write-wins 注册表
  逻辑（pipeline.rs:557-558, 1075-1076），无 LLM 判断点，Jev 无处挂载。
- **digest 触发**：纯规则阈值（trigger.rs:224），非 LLM 判断。
- **summary / edge_text 撰写等生成环节**：Jev 不生成文本，原理上不适用。
- **参与 gate（是否回复）**：非 memory 子系统，超出本实验范围。

## 5. 指标体系（复用 jev-demo `jev_bench/metrics.py`，纯 stdlib）

accuracy / macro-F1 / AUROC（rank-based）/ ECE / Brier / 可靠性分箱 /
AURC（risk-coverage）/ Spearman / flip rate / mean pairwise deviation /
延迟分位 / 成本（按 usage.input_tokens × $0.042/Mtok）。金标与参照标签
**分别出表**，不混用（参照是 haiku 判断，非真值）。

## 6. Baselines

1. **向量阈值规则**（现状的硬门槛部分）：sim≥0.88 即并 / sim≥0.90 即
   候选——展示「一刀切」在各 τ 下的 P/R 曲线作为地板。
2. **haiku 现状**（J1/J2/J3 的现行实现）：实时重判作为 ceiling 参照。
3. **Jev**：挑战者。所有指标三方并列。

## 7. Harness 架构（本分支新增）

```
tamako-jev-lab/                # workspace member（仅本分支）
  src/main.rs                  # extractor：只读打开快照副本 → JSONL fixtures
                               #   rusqlite(+sqlite-vec 注册) 读 store.db：
                               #   messages / edge_texts / injected_memories /
                               #   node_embeddings 全量向量（vec0 全表扫描）
                               #   lbug crate 直读 memory.lbug：
                               #   MATCH (n:Node) / MATCH ()-[r:EDGE]->() 全量
  driver/                      # Python 3（桌面 host，stdlib + requests）
    client.py                  # 移植 jev-demo OpenRouterBackend（重试/退避）
    metrics.py                 # 移植 jev_bench/metrics.py（注明出处）
    suites.py                  # UC-1a/1b/2/3/4/5 任务构建器（fixtures → tasks）
    haiku_ref.py               # haiku 参照判定（OPENROUTER_API_KEY 直连）
    run.py                     # 编排：--suites --mock --limit --out runs/<ts>/
  fixtures/                    # .gitignore（含私密群聊内容，永不提交）
  runs/                        # .gitignore（raw.jsonl 含原文，永不提交）
docs/jev-memory-benchmark.md   # 本文档
docs/jev-memory-report.md      # 交付物 2：实验报告（仅聚合指标，无原文）
```

纪律：fixtures/runs 一律不进 git（私密内容）；报告只含聚合数值与计数；
分支永不推送；OPENROUTER_API_KEY 只从环境变量读取。

## 8. 样本量与成本估算

| 套件 | 请求数估 | 单请求输入估 | 成本估 |
|---|---|---|---|
| UC-1a | ≤500 noul（fan-out 后 ~50 请求） | ~400 tok | < $0.01 |
| UC-1b | ≤300 noul | ~400 tok | < $0.01 |
| UC-2 | ≤200 choice | ~400 tok | < $0.01 |
| UC-3 | 60 窗 × ≤40 noul = ≤2400（60 请求） | ~4k tok | < $0.02 |
| UC-4 | 60 noul | ~4k tok | < $0.01 |
| UC-5 | 复用 + 30 对照 | — | < $0.01 |
| haiku 参照 | ~800 调用 | — | < $1 |
| **合计** | | | **< $1.5** |

## 9. 风险与解读边界

1. **CJK 准确率**：guidebook 明示非英语负载需自有数据验证——UC-5 专答。
2. **参照标签非真值**：haiku 重判自身有误差；金标仅人工子集，结论以
   金标为准、参照为辅。
3. **alpha 端点**：OpenRouter decisions 为 alpha，行为可能与原生 API 有
   差异；响应 model 字段逐请求记录以便版本审计。
4. **快照时效**：数据冻结于 2026-09-12；retrieval 重放的候选组装是
   recall.rs 的离线复刻（简化：无 warmup/冷却逻辑），与在线路径存在
   已知偏差，结论表述为「候选质量层」而非「端到端」。
5. **结构恒等式不成立**（jev-demo 实测复现）：不假设 P(¬q)=1−P(q)，
   阈值不跨原语搬运（guidebook §6.4）。
