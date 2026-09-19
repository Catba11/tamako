# 记忆图谱数据质量清理：根因、修复与验证（终版 v2）

日期：2026-09-18 ｜ 分支：jev-memory（仅桌面 worktree，不推送）
范围：测试副本 `~/jev-lab/data`（8 群 lbug graph + store.db 旁表）；生产 `/var/lib/tamako` 全程只读。
状态：三项裁决已批准并落地；补丁全测通过；测试副本清理三阶段全部完成并断言。
v2 修订：采纳八条顾问复核意见（spec 定性、D2 哨兵副作用、step-1 存名、旁表同步、
护栏矛盾、表格量纲、验收判据声明、DoD 四件套），详见 §9。

## 1. 背景

Jev 实验（docs/jev-memory-report.md）发现候选列表被 `"X is a surface form of X"` 同义反复
与 `"batch X-Y mentions Z"` 骨架边"污染"。结论：contains 边是 spec 明示的 provenance-only
结构（§5，零改动）；别名边问题是三层机制叠加（§2），修复已获裁决并落地（§6）。

## 2. 根因实证（代码 + 图数据双重证据）

### L1 模板硬编码
`resolve.rs` 别名边构造：`format!("{} is a surface form of {}.", extracted.name, extracted.name)` —— 两槽同一变量。图数据：16,061/16,061 = 100% 同义反复。

### L2 实体名不稳定 —— 定性为代码/spec 冲突
spec `proposed-graph-database-specs.md:119` 明写 `Node.name` = **"Canonical name of the entity"**。
而 MERGE_NODE 的 `ON MATCH SET n.name = $name` 使实体名随每次再绑定漂移为最新表层形式
（图数据：实体名==别名表层形式占 97.6%）。**这是代码违反 spec 的冲突，非可自由选择的口径**；
D1 定性为冲突修复。另：`ResolvedEntity` 不携带规范名、`ALIAS_TARGETS` 不返回存名，是文本
无法信息性的接线缺口。

### L3 别名边按批增殖 + step-2 绑定失效（双重危害）
MERGE_EDGE 自然键含 `valid_at`，别名边每批以新 `batch_end` 重写 → MERGE 永不命中 → 每批
新增一行。危害一：膨胀（16,061 行 / 8,412 唯一键，单键最高 682 重复）。危害二（更重）：
step-2 精确绑定要求 `alias_targets` 恰返回单元素，重复行使长度 >1 → step-2 静默失败 →
实体落 step-4 备用附着。**凡重复过的别名，其精确绑定在生产上已长期失效。**

## 3. 测量汇总（单位：行=edge 行，键=唯一 (源,目标,关系) 绑定键）

| 指标（单位） | 清理前 | 清理后 |
|---|---|---|
| 别名边总数（行） | 16,061 | **8,412** |
| 唯一绑定（键） | 8,412 | 8,412（不变） |
| 重复（行） | 7,649 | **0** |
| 事实性过期文本（行） | 472 | **0** |
| 真实同义反复（行）* | 15,676 | 8,269 |
| 信息性文本（行） | 385 | **143**（含全部 472 修正中经去重留存者） |
| store.db 旁表别名行（行） | 16,061（含漂移/旧键） | **8,412（与图 1:1）** |
| valid_at 未对齐哨兵（行） | 16,061 | **0** |

\* "真实同义" = 实体当前名恰等于该表层形式，文本 `"X is a surface form of X."` 为真陈述
（新建实体的规范名即首见表层形式，此类同义是模型内生的，见 §9 判据重定义声明）。

## 4. 已执行：测试副本清理（三阶段，工具 alias_backfill）

工具 `tamako-jev-lab/src/bin/alias_backfill.rs`：dry-run 默认；`--apply` 写入；
`--allow-production` 显式解除生产路径护栏（§7 使用）；`--dedupe` / `--align-valid-at` / `--sidecar`。

**阶段一 文本回填**：472 行过期文本按当前图状态重渲染为真实陈述（含跨语言桥案例）。
断言：过期=0 ✓ 行/键不变 ✓ 拓扑哈希不变 ✓ 可重入 ✓。

**阶段二 去重**：每键保留最早 `valid_at` 行，删 7,649 行。
断言：16,061→8,412 ✓ 键不变 ✓ 单键最大=1 ✓ 文本不受影响 ✓ 二次运行删 0 ✓。
附带修复 L3 危害二：全部别名键恢复单目标。

**阶段三 哨兵对齐 + 旁表同步**：
- `--align-valid-at`：8,412 行 `valid_at` 统一为 `1970-01-01T00:00:00Z`（结构绑定哨兵）；
- `--sidecar`：store.db `edge_texts` 旁表（decision 76 的 LIKE 检索镜像，别名行 16,061）
  按自然键重键（valid_at 变更）+ 漂移重写 + 垂悬清理 → 收敛至 8,412 行，与图 1:1。
断言：misaligned=0 ✓ 旁表垂悬=0 ✓ 重键余量=0 ✓ 文本漂移=0 ✓ 逐群旁表==图 ✓。
（生产侧该旁表本可由 reconciliation 按内容漂移自愈（decision 77 S6-F6），工具选择立即同步，
不依赖收敛等待。）

快照：`~/jev-lab/data.pre-backfill-20260918`（回滚 = 整体恢复）。

## 5. contains provenance 边：规范冲突上报（零改动）

spec §6.3/§8.2 明示 provenance-only，recall 白名单已排除（spec:276）。出现在 Jev 候选列表
系实验 harness 全量枚举口径，非生产污染。不删、不改。

## 6. 已落地：生成器修复（D1/D2/D3 + 两项加固）

| 决策 | 实现 | 位置 |
|---|---|---|
| D1 稳定规范名（spec:119 冲突修复） | MERGE_NODE 的 ON MATCH 移除 `n.name = $name`（name 列 create-only） | tamako-memory |
| D2 结构绑定 | 别名边 `valid_at = OffsetDateTime::UNIX_EPOCH` 哨兵（与批次无关 → 任意路径 MERGE 原地命中，**这是止增殖的实际机制**）；`bound_via_alias` 跳过重写仅作幂等优化 | tamako-agent |
| D3 规范名接线 | `ALIAS_TARGETS` 增返 `s.name`；`ResolvedEntity.canonical_name`：step-1 读存名（`node_content`，首提回退表层形式）、step-2 取存名、step-3 取 `node_content` 存名、新节点取自身 | 两 crate |
| 加固 1 | `ALIAS_TARGETS → RETURN DISTINCT`：重复行再现也不再打破单目标匹配 | tamako-memory |
| 加固 2 | `TWO_HOP_EDGES` 时间窗豁免 `also_known_as`：哨兵 valid_at + created_at 只写一次，否则超窗（90 天）的跨语言桥会静默掉出深召回（decision 74/76） | tamako-memory |

**D2 的 spec 定性（须经独立 spec 修正，非复用单值注册表）**：spec:210-213 谓词注册表默认
multi-value，`known_as`/`also_known_as` 不在 `single_value_predicates`（decision 75）。注意
**不可**走"把别名谓词登记进 `single_value_predicates`"这条路：spec:212 的单值语义是"每个
(subject, predicate) 只许一条有效边"，§7.5:215-218 的失效动作会给同 subject 同 predicate 的
其它有效边置 `invalid_at`——套到别名边上即"每实体只准一个别名"，直接毁掉多表层形式（正是
§6.3 跨语言桥要保的结构）。D2 因此需要一条**独立的 spec 修正**：新增"**结构绑定**"谓词类——
valid_at 取批次无关哨兵、不参与多值累加、不适用 §7.5 单值失效语义。合入主线时的文档义务：
current-state.md 决策条目 + specs.md 新增结构绑定类条文（而非 §13 注册表登记）；7,649 行
去重 + 8,412 行哨兵对齐按**存量数据迁移**对待（本报告 §4 即迁移记录与验证）。

**回归测试（7 个，全部具名通过）**：
- `resolve::tests::a_mention_binding_renders_the_canonical_alias_text`（D3/L1）
- `resolve::tests::an_alias_bound_entity_does_not_rewrite_the_alias_edge`（D2 skip 幂等）
- `resolve::tests::repeated_batches_do_not_duplicate_the_alias_edge`（D2 哨兵：跨批单行）
- `lbug_backend::tests::a_rebind_does_not_rename_the_node`（D1）
- `lbug_backend::tests::alias_targets_dedupes_repeated_binding_rows`（DISTINCT）
- `lbug_backend::tests::two_hop_edges_structural_alias_bridges_never_age_out`（窗口豁免，
  对照：同龄事实边仍被窗口过滤）
- （另：既有 `alias_targets_*` 两测试随 name 字段更新）

**测试**：`cargo test -p tamako-agent` 318+13 ✓；`cargo test -p tamako-memory` 77 ✓；
`cargo test --workspace` 见 §9 DoD 记录。

## 7. 生产执行预案（2026-09-18 已执行）

1. 补丁合入主线（注意当前在 jev-memory 实验分支）+ current-state.md/specs.md 文档条目；
2. `systemctl stop tamako.service`（lbug 单写者）；
3. 备份 `/var/lib/tamako`；
4. `alias_backfill --data-root /var/lib/tamako --allow-production --apply`（文本回填+断言）；
5. `alias_backfill --data-root /var/lib/tamako --allow-production --apply --dedupe`（去重）；
6. `alias_backfill --data-root /var/lib/tamako --allow-production --apply --align-valid-at --sidecar`（哨兵对齐+旁表同步）；
7. `systemctl start tamako.service`；回滚 = 恢复步骤 3 备份。

执行记录（2026-09-18，与 decision-123 v15 迁移同一窗口，operator 批准）：
- 镜像：`localhost/tamako:fb104ed`（按 AGENT.md §6.6 从 `catball-self-use`
  分支 tip 构建，非 main）；备份：`~/tamako-backup-20260918/data`（573M）。
- 步骤 4：stale_rows=780，pending_keys=230，applied。步骤 5：dedup_deleted=9149。
- 步骤 6：exit 0；旁表计数（步骤后 `SELECT COUNT(*) FROM edge_texts`，只读）：
  4311 / 27913 / 884 / 3357 / 4766 / 5000 / 5399（七个真实群；测试群 0，无别名）。
- 窗口前发现并已修复（Runtime blocker，a76460b）：群发现曾仅按 is_dir 过滤，
  会对 Frameworks//bugscope/ 这类非群目录创建垃圾 memory.lbug；现要求目录内
  存在 store.db（群规范标记）。生产三步确认 Frameworks//bugscope/ 零写入。
- 已闭环（原 Runtime concern，2026-09-18 当日两条证据）：(1) 新镜像启动和解
  （decision 77 S6-F6）对 8 群全部报 edge_texts_upserted=0 /
  edge_texts_pruned=0 / enqueued=0——旁表与图零漂移，embedding 零补录；
  (2) 对窗口前备份（~/tamako-backup-20260918）做 dry run 交叉复核，
  stale_rows=780 / pending_keys=230 / dedup_deleted=9149 与生产三步计数器
  逐一相符。去重规模：18,946 行别名边 → 9,797 个不同绑定（48.3% 重复，
  单绑定最多 315 行重复；tautological 自指行 18,270，占 96%——规范名漂移
  的退化产物）。无需再安排 dry run 停服。

## 8. 复现

```bash
toolbox run -c tamako-spike cargo test -p tamako-agent resolve   # 40+
toolbox run -c tamako-spike cargo test -p tamako-memory          # 77
toolbox run -c tamako-spike cargo build -p tamako-jev-lab --bin alias_backfill
./target/debug/alias_backfill --data-root ~/jev-lab/data \
    [--apply] [--dedupe] [--align-valid-at] [--sidecar] [--allow-production]
```

## 9. 顾问复核采纳记录与验收判据声明

**验收判据重定义（显式声明）**：原目标判据 #3.1"同义反复模式命中数 = 0"经 L2 实证
不可达——新建实体的规范名即其首见表层形式，此类 `"X is a surface form of X."` 是真实
陈述，任何回填都无法也无须消除。判据就地修订为"**事实性过期文本 = 0**"（已达成），
剩余真实同义 8,269 行属设计内。此修订经由本报告向运营者明示，非静默替换。

**采纳的复核意见**：D1 改定性为 spec 冲突（spec:119）；D2 的 spec 路径从"注册表登记"
更正为独立 spec 修正（新增结构绑定谓词类，:212/:215-218 单值语义会毁多表层形式）与迁移定性；D2 初版只堵 step-2 路径的缺陷改为哨兵 valid_at；哨兵对深召回时间窗的副作用
以谓词豁免修复；step-1 规范名改读存名；store.db 旁表纳入同步；工具护栏与 §7 的矛盾以
`--allow-production` 解决；§3 表格补单位；DoD 四件套（fmt/clippy/build/test --workspace）
纳入交付门禁。

证据锚点：resolve.rs（ResolvedEntity、step-5 哨边、canonical 接线）；lbug_backend.rs
（MERGE_NODE、ALIAS_TARGETS、TWO_HOP_EDGES）；proposed-graph-database-specs.md:119,
151-152, 168, 204, 210-213, 276。
