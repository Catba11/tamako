# 记忆图谱数据质量清理：根因、修复与验证（终版）

日期：2026-09-18 ｜ 分支：jev-memory（仅桌面 worktree，不推送）
范围：测试副本 `~/jev-lab/data`（8 群 lbug graph 拷贝）；生产 `/var/lib/tamako` 全程只读。
状态：**三项裁决已由运营者批准（D1=稳定规范名、D2=结构绑定+去重、D3=接线），生成器补丁已实现并全测通过，测试副本清理已完成。**

## 1. 背景

Jev 实验（docs/jev-memory-report.md）发现候选列表被两类模式"污染"：
`"X is a surface form of X"` 同义反复文本与 `"batch XXXX-YYYY mentions Z"` 骨架边。
结论：两者都不是"删数据"能解决的；`contains` 边为 spec 明示的 provenance-only 结构（§5，零改动）；
别名边问题是三层机制叠加（§2），修复需要并已获得 spec 级裁决（§6）。

## 2. 根因实证：三层机制（代码 + 图数据双重证据）

### L1 模板硬编码（影响 100% 别名边）
`tamako-agent/src/resolve.rs:527-530`：`format!("{} is a surface form of {}.", extracted.name, extracted.name)` —— 两个槽位同一个变量。图数据：16,061/16,061 = 100% 同义反复。

### L2 实体名不稳定（决定"修复文本"的可行性）
- `tamako-memory/src/lbug_backend.rs` MERGE_NODE：`ON MATCH SET n.name = $name` —— 每次再绑定把实体改名为最新表层形式；
- `entity_node`（resolve.rs）所有路径无条件传 `extracted.name`；
- `ResolvedEntity` 不携带规范名；`ALIAS_TARGETS` 只返回 `s.id, s.type`。

图数据：实体当前名 == 别名表层形式占 97.6%（15,676/16,061）——"规范名"不是稳定列，只修模板无意义。

### L3 别名边按批增殖（结构膨胀 + 绑定失效，双重危害）
MERGE_EDGE 自然键含 `valid_at`，别名边每批以新 `batch_end` 重写 → MERGE 永不命中 → 每批新增一行。

**危害一（膨胀）**：16,061 行仅 8,412 个唯一绑定键，重复 7,649 行；最热键单键重复 682 次。
**危害二（绑定失效，比膨胀严重）**：step-2 精确绑定要求 `alias_targets` 恰好返回单元素
（`if let [target] = targets.as_slice()`）。重复行使返回长度 >1 → step-2 静默失败 →
实体落入 step-4 备用附着（attached_to_alias，不绑定真实节点）。**即：凡发生过重复的别名，
其 step-2 精确绑定在生产上已长期失效**（含重复 682 次的运营者本人别名键）。

### 失效别名边
0 行（invalid_at 全空）——重复行不携带任何增量信息。

## 3. 测量汇总（测试副本，8 群合计）

| 指标 | 清理前 | 清理后 |
|---|---|---|
| 别名边总行数 | 16,061 | **8,412** |
| 唯一绑定键 | 8,412 | 8,412（不变） |
| 重复行 | 7,649 | **0** |
| 文本同义反复 | 16,061（100%） | 15,676（均为"实体当前名==表层形式"的真实同义，属设计内） |
| 事实性过期文本 | 472 行 / 211 键 | **0** |
| 单键最大重复 | 682 | **1** |

## 4. 已执行：测试副本清理（工具 + 验证）

工具：`tamako-jev-lab/src/bin/alias_backfill.rs`（本分支；dry-run 默认；`--apply` 写入；
`--dedupe` 去重；内置护栏拒绝指向 `/var/lib/tamako`）。

**阶段一 文本回填**（已执行）：有效别名边 edge_text 按当前图状态重渲染为
`"{表层形式} is a surface form of {实体当前名}."` —— 472 行过期文本变为真实且信息性的陈述，
含跨语言案例（正对应 spec §6.3 跨语言桥意图）。
断言：A 过期=0 ✓ B 行数/键数不变 ✓ C 拓扑哈希不变 ✓ D 二次运行零变更 ✓。

**阶段二 去重**（已执行）：每绑定键保留最早 `valid_at` 行，删除 7,649 行重复。
断言：rows 16,061→8,412 ✓ 键数不变 ✓ duplicate=0 ✓ max=1 ✓ 文本不受影（stale=0）✓
二次 `--dedupe` 删 0 ✓。去重同时**修复危害二**：所有别名键恢复单目标，step-2 绑定重新可用。

快照：`~/jev-lab/data.pre-backfill-20260918`（回滚 = 整体恢复）。
报告 JSON：`~/jev-lab/report_{pre,apply1,post,apply2,dedupe_pre,dedupe_apply1,dedupe_post,dedupe_apply2}.json`。

## 5. contains provenance 边：规范冲突上报（零改动）

`resolve.rs:595-607` 的 `contains` 边是 spec §6.3/§8.2 明示的 provenance-only 结构
（recall 白名单已排除，`proposed-graph-database-specs.md:276`）。出现在 Jev 候选列表是
实验 harness 全量枚举口径所致，非生产 recall 污染。不删、不改。

## 6. 已批准并落地：生成器修复（D1/D2/D3）

| 决策 | 实现 | 位置 |
|---|---|---|
| D1 稳定规范名 | MERGE_NODE 的 ON MATCH 移除 `n.name = $name`（name 列 create-only） | tamako-memory/src/lbug_backend.rs |
| D2 结构绑定 | `ResolvedEntity.bound_via_alias`：step-2 经别名绑定的实体跳过别名边重写（首次绑定才写） | tamako-agent/src/resolve.rs |
| D3 接线 | `ALIAS_TARGETS` 增返 `s.name`；`AliasTarget.name`；`ResolvedEntity.canonical_name`；模板第二槽填规范名（step-1 取 display_name，step-2 取存名，step-3 取 node_content 存名，新节点取自身） | 两 crate |
| 加固 | `ALIAS_TARGETS` → `RETURN DISTINCT`：即使将来再现重复行，step-2 绑定也不再失效 | tamako-memory |

**回归测试**（全部具名通过）：
- `resolve::tests::an_alias_bound_entity_does_not_rewrite_the_alias_edge`（D2：绑定后不重写边 + 存边完好）
- `resolve::tests::a_mention_binding_renders_the_canonical_alias_text`（D3/L1：第二槽为规范名）
- `lbug_backend::tests::a_rebind_does_not_rename_the_node`（D1：再绑定不改名）
- `lbug_backend::tests::alias_targets_dedupes_repeated_binding_rows`（DISTINCT 加固）

**测试全绿**：`cargo test -p tamako-agent` 317+13 passed / 0 failed；
`cargo test -p tamako-memory` 76 passed / 0 failed；lab crate 构建通过。

## 7. 生产执行预案（待批准，本任务未执行）

1. 发布含 §6 补丁的版本（注意：本分支为实验分支，补丁需择分支合入主线）；
2. `systemctl stop tamako.service`（lbug 单写者）；
3. 备份 `/var/lib/tamako`；
4. `alias_backfill --data-root /var/lib/tamako --apply`（文本回填 + 四断言）；
5. `alias_backfill --data-root /var/lib/tamako --apply --dedupe`（去重 + 断言 rows==keys）；
6. `systemctl start tamako.service`；回滚 = 恢复步骤 3 备份。
预期效果：别名边 16,061→8,412 行；全部别名键 step-2 绑定恢复；新产生文本自带规范名且不再增殖。

## 8. 复现

```bash
# 桌面 worktree ~/tamako-jev，分支 jev-memory
toolbox run -c tamako-spike cargo test -p tamako-agent resolve   # 40 个 resolve 测试
toolbox run -c tamako-spike cargo test -p tamako-memory          # 76 个
toolbox run -c tamako-spike cargo build -p tamako-jev-lab --bin alias_backfill
./target/debug/alias_backfill --data-root ~/jev-lab/data [--apply] [--dedupe]
```

证据锚点：resolve.rs（ResolvedEntity、step-5 skip、edge_text）；lbug_backend.rs（MERGE_NODE、
ALIAS_TARGETS、DISTINCT）；proposed-graph-database-specs.md:151-152, 168, 204, 276。
