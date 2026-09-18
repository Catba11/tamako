# 记忆图谱数据质量清理：根因实证、回填验证与行动方案

日期：2026-09-18 ｜ 分支：jev-memory（仅桌面 worktree，不推送）
范围：测试副本 `~/jev-lab/data`（8 群 lbug graph 拷贝）；生产 `/var/lib/tamako` 全程只读，未做任何改动。

## 1. 背景

Jev 实验（docs/jev-memory-report.md）发现候选列表被两类模式"污染"：
`"X is a surface form of X"` 同义反复文本与 `"batch XXXX-YYYY mentions Z"` 骨架边。
本文件记录对这两项的根因实证、已验证的回填方案，以及需要你裁决的生成器修复决策点。

**结论先行**：两项都不是"删数据"能解决的。别名边同义反复是三层机制叠加（§2），
其中稳定修复需要 spec 级裁决（§6）；`contains` 边是 spec 明示的 provenance-only 结构，
已在 recall 白名单之外，**零改动、仅上报**（§5）。测试副本上的文本回填已执行并通过全部断言（§4）。

## 2. 根因实证：三层机制（代码 + 图数据双重证据）

### L1 模板硬编码（表层原因，影响 100% 别名边）

`tamako-agent/src/resolve.rs:527-530`：

```rust
edge_text: format!(
    "{} is a surface form of {}.",
    extracted.name, extracted.name   // 两个槽位是同一个变量
),
```

每条 `known_as`/`also_known_as` 边在创建时文本就是同义反复。图数据：16,061/16,061 = **100%**。

### L2 实体名不稳定（深层原因，决定"修复文本"的可行性）

- `tamako-memory/src/lbug_backend.rs:97-99`（MERGE_NODE）：`ON MATCH SET n.name = $name` —— 每次再绑定都把实体节点**改名为最新表层形式**；
- `resolve.rs:1183/1193/1202`（entity_node）：所有解析路径无条件传 `name: extracted.name`；
- `resolve.rs:356-373`（ResolvedEntity）：不携带任何规范名字段；`ALIAS_TARGETS`（lbug_backend.rs:129-131）只返回 `s.id, s.type`，**解析流程拿不到实体的存名**。

图数据：实体当前名 == 别名表层形式的行占 **97.6%**（15,676/16,061）。
推论：模板要的"规范名"在库里根本不是一个稳定列。只修 L1 模板没有意义——97.6% 的情况下填什么都是同义反复。

### L3 别名边按批增殖（结构膨胀）

`MERGE_EDGE`（lbug_backend.rs:119-122）的自然键含 `valid_at`，而 `resolve.rs:525` 给别名边
`valid_at: batch_end`——同一表层形式每被提取一次就新增一行，MERGE 永不命中。
图数据：16,061 行对应 8,412 个唯一 (源, 目标, 关系) 键，**重复行 7,649（膨胀 1.91×）**；
最热键单键重复数百次。`invalid_at` 全为空（0 行失效别名边），重复行不携带任何增量信息。

### 三重奏的净效果

L1 让文本天生无用；L2 让"有用的文本"无处可取；L3 让无用文本按批复制。
下游症状即 Jev 实验所见：候选列表里塞满同义反复句，判断模型拿不到别名关系的有效上下文。

## 3. 测量汇总（测试副本，8 群合计）

| 指标 | 值 |
|---|---|
| 别名边总行数（known_as + also_known_as） | 16,061（6,582 + 9,479） |
| 失效别名边 | 0 |
| 唯一 (源, 目标, 关系) 键 | 8,412 |
| 重复行（L3 增殖） | 7,649 |
| 文本同义反复（L1） | 16,061（100%） |
| 实体名 == 别名表层形式（L2 改名重合） | 15,676（97.6%） |
| 文本已过期（实体已改名，旧文本事实性错误） | **472 行 / 211 键** |

## 4. 回填工具与验证（已在测试副本执行）

工具：`tamako-jev-lab/src/bin/alias_backfill.rs`（本分支提交；dry-run 默认，`--apply` 才写；
内置护栏：拒绝指向 `/var/lib/tamako` 的 --data-root）。

动作：把每条**有效**别名边的 edge_text 按当前图状态重渲染为
`"{别名表层形式} is a surface form of {实体当前名}."`。
对实体当前名恰等于表层形式的 15,676 行，新旧文本相同（不动）；对已过期的 472 行，
文本变为真实且信息性的陈述。典型修正形态（真实样本已在本地验证，此处示意）：
`"昵称A is a surface form of 昵称A."` → `"昵称A is a surface form of 规范名B."`，
含跨语言案例（中文俚称 → 中文规范名、英文 → 中文），正对应 spec §6.3 跨语言桥的设计意图。

**四断言验证结果（全部 PASS）**：

| 断言 | 结果 |
|---|---|
| A 回填后过期文本 = 0 | PASS（472 → 0，pending_keys 211 → 0） |
| B 行数/键数不变 | PASS（16,061 行 / 8,412 键，前后一致） |
| C 拓扑不变 | PASS（每群 (源,目标,关系,valid_at) 多重集哈希前后一致） |
| D 可重入 | PASS（二次 --apply 变更 0 行） |

快照：`~/jev-lab/data.pre-backfill-20260918`（366M，回填前完整拷贝，回滚 = 整体恢复）。
报告 JSON：`~/jev-lab/report_{pre,apply1,post,apply2}.json`。

## 5. contains provenance 边：规范冲突上报（零改动）

`"batch X-Y mentions Z"` 文本来自 `resolve.rs:595-607` 的 `contains` 边，是 spec §6.3/§8.2
明示的 provenance-only 结构（recall 白名单已排除，`proposed-graph-database-specs.md:276`）。
它们出现在 Jev 候选列表里是**实验 harness 的枚举口径**（全量边 dump）所致，
不是生产 recall 的污染。建议：不删、不改文本；若希望 harness 更贴近生产 read path，
在提取器侧按 §8.2 白名单过滤（实验已结束，非必须）。

## 6. 待裁决：生成器修复的三个决策点

命中本次任务的停止条件（修复涉及 spec 解释与跨 crate 改动），请拍板：

**D1 实体 `name` 列的语义** —— 这是所有修复的地基。
- 选项 A（推荐）：**稳定规范名**。`MERGE_NODE` 的 `ON MATCH` 不再 SET `n.name`（首见名即规范名；
  改名只经显式 merge 工具）。影响：tamako-memory 私有查询常量（非接口）；recall 渲染的名字不再
  随最新表层形式漂移；历史改名不可恢复（以当前名为稳定起点，接受）。
- 选项 B（保守）：维持"最新表层形式"语义。则 edge_text 永远只能对 ~3% 的边提供信息，
  别名边文本长期价值趋零，L1 修复降格为装饰。

**D2 别名边的时间语义** —— 决定 7,649 行膨胀去留。
- 选项 A（推荐）：**结构绑定**。别名边是绑定不是事实：`valid_at` 不再取 `batch_end`
  （用节点创建时或固定值），生成器对既有有效绑定跳过重写；一次性去重 16,061 → 8,412 行。
  需确认 spec §7.5"every predicate is multi-value"不适用于绑定类边（§6.3 的措辞支持此读法）。
- 选项 B（保守）：维持多值，接受膨胀（每批 +N 行，增长率与 digest 频率成正比）。

**D3 信息性文本的接线**（D1=A 才有意义）。
`ALIAS_TARGETS` 增返 `s.name`（跨 crate trait 返回结构改动）或新增按 id 批量取名方法；
`ResolvedEntity` 携带规范名；`resolve.rs:527-530` 第二槽填规范名。
回归测试：构造 bound_to_existing 且存名 ≠ 表层名的用例（现有测试 resolve.rs:1365 两个名字
恰好相同，所以抓不到 L1）。验证命令：`cargo test -p tamako-agent resolve`。

推荐组合：**D1=A + D2=A + D3**。理由：三者正交且互相成就——D1 提供稳定规范名，
D2 止住膨胀，D3 让新旧文本都信息性；回填工具已验证，重跑即可让所有文本对齐新语义。

## 7. 生产执行预案（待批准，本任务不执行）

1. `systemctl stop tamako.service`（lbug 单写者，必须与 bot 串行）；
2. 备份 `/var/lib/tamako`（同 §4 快照方式）；
3. `alias_backfill --data-root /var/lib/tamako --apply` + 四断言复核；
4. （若 D2=A）别名边去重 + 生成器补丁发布后，由重放/回填对齐存量；
5. `systemctl start tamako.service`；回滚 = 恢复步骤 2 的备份。

## 8. 复现

```bash
# 桌面 worktree ~/tamako-jev，分支 jev-memory
toolbox run -c tamako-spike cargo build -p tamako-jev-lab --bin alias_backfill
./target/debug/alias_backfill --data-root ~/jev-lab/data            # dry-run
./target/debug/alias_backfill --data-root ~/jev-lab/data --apply    # 写入
```

证据锚点：resolve.rs:356-373, 521-535, 595-607, 1142-1215；lbug_backend.rs:97-99, 119-131；
proposed-graph-database-specs.md:151-152, 168, 204, 276。
