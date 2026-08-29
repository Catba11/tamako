# Test Coverage 评估 — `catball-self-use` 分支（Phase 1 之后）

生成时间：2026-08-28。范围：merge-base `9161966` 以来的 24 个 commit（decisions 81–84）。
本文件是操作员要求的分析产物，不是治理文档。

## 测量方法

- 命令：`cargo llvm-cov --workspace --html --output-dir target/llvm-cov-html`（HTML 报告在 `target/llvm-cov-html/html/index.html`），机器可读数据在 `target/llvm-cov.json`（`cargo llvm-cov --workspace --json`）。
- 运行的是默认测试集。`--ignored` 的 live 探针**未参与**本轮测量（符合预期，它们需要真实 API key）：
  - `tamako-agent/tests/caption_spike.rs:41`、`tamako-agent/tests/embedding_spike.rs:52`、`tamako-agent/tests/live_wake.rs`、`tamako-agent/tests/live_extraction.rs`、`tamako/tests/lbug_cross_process.rs`、`tamako/src/persona_watch.rs:419`。
- 全部测试通过（0 失败）。

## 总体数字

| 指标 | 覆盖率 |
|---|---|
| Regions | **92.26%**（4494 / 58083 missed）|
| Lines | **91.74%**（3041 / 36819 missed）|
| Functions | **88.66%**（389 / 3431 missed）|

## 按 crate 汇总

| Crate | Regions | Functions | Lines | 评价 |
|---|---|---|---|---|
| tamako-persona | 99.79% | 100.00% | 100.00% | 优秀 |
| tamako-core | 97.50% | 92.35% | 97.65% | 优秀（context.rs 99.86%、wake.rs 100%、merge.rs 98.78%）|
| tamako-vision | 97.64% | 100.00% | 95.09% | 优秀（新 crate，含 no-panic 模糊输入测试）|
| tamako-adapter-mock | 96.86% | 96.55% | 98.31% | 优秀 |
| tamako-memory | 95.84% | 85.68% | 93.93% | 表面高，但集中在 lbug_backend.rs（97.97% lines）；**backend.rs 只有 28.32% lines** |
| tamako-store | 93.93% | 97.23% | 96.51% | 良好（store.rs 96.70% lines；schema.rs 86.96%）|
| tamako-agent | 94.05% | 89.62% | 92.46% | 良好，但 rig 薄壳层（rig_impl/caption/summary/warmup 的 Endpoint* 构造与 live 调用）未被覆盖 |
| tamako-adapter-teloxide | 88.31% | 88.26% | 87.11% | normalize.rs 98.07% 很高；**media.rs 71.20%、adapter.rs 80.26%** 拖低 |
| **tamako（二进制）** | **56.96%** | **56.56%** | **54.88%** | 最弱；main.rs 54.26% lines、persona_watch.rs 61.11% lines |

行覆盖率最低的文件：`tamako-memory/src/backend.rs` 28.32%、`tamako/src/main.rs` 54.26%、`tamako/src/persona_watch.rs` 61.11%、`tamako-agent/src/rig_impl.rs` 68.55%、`tamako-adapter-teloxide/src/media.rs` 71.20%、`tamako-agent/src/caption.rs` 77.15%、`tamako-adapter-teloxide/src/adapter.rs` 80.26%、`tamako-agent/src/warmup.rs` 82.14%、`tamako-agent/src/summary.rs` 82.18%、`tamako-agent/src/merge_confirm.rs` 83.42%、`tamako-store/src/schema.rs` 86.96%。

##  ranked 缺口清单（新代码优先，带 file:line 证据）

### G1（最高优先级）— media 富化编排路径在默认测试中完全未覆盖

`tamako-adapter-teloxide/src/media.rs` 里所有带 I/O 的函数执行计数为 0：`download_media`（media.rs:286-305）、`caption_media_file`、`caption_of_media_file`、`sticker_element`、`cache_sticker_caption`、`enrich_media_message`、`media_element_for`、`caption_data_uri`、`sticker_cache_miss`。已测的只有纯拼装/placeholder 辅助函数（media.rs:406/481/505 一带的测试）和 normalize.rs 的分类逻辑（98%）。唯一的端到端验证是 `#[ignore]` 的 live caption_spike。

**后果**：本次质量审查发现的真实缺陷——下载超时不收敛、provider 错误路径、`mentions_bot` 不读 `caption_entities`（normalize.rs:425-437）、编辑消息把 caption 降级为空 placeholder（media.rs:56-79）——全部位于这个未被默认测试触及的区域。decision 82 的"一切失败路径收敛到 placeholder"的核心不变量没有离线回归保障。

**建议**：在 adapter crate 内补 seam 级测试：用 `tamako_core::caption::ScriptedCaption`（现成的 scripted double）+ 假下载源驱动 `MediaEnricher`，断言：下载失败→placeholder、provider `CaptionError::Empty`→placeholder 且不重试、`Provider` 错误→placeholder、sticker 缓存命中/未命中计数。级别：单元/集成（不需要网络）。**为什么重要**：这是 P1 日志内容的唯一来源路径，它的失败语义目前只靠 live 探针抽检。

### G2 — `apply_session_suffixes` 与 provider 构造接线（main.rs）零覆盖

`tamako/src/main.rs:620-647` 的 `apply_session_suffixes` 执行计数为 0；`build_media_enricher`、`build_caption_provider`、`build_embedding_provider`、`run_live`、`run_replay` 等整个 composition root 均未执行（main.rs 整体 54.26% lines）。decision 84 的 `{prefix}-{suffix}` 拼接、四个 purpose 的遍历、mint 失败即 fatal 的错误臂都没有测试。

**已有的好覆盖**：双 header 的**线上行为是真 wire-tested 的**——`tamako-agent/src/endpoint.rs:2781 the_session_id_header_reaches_the_wire` 起真实 TCP listener，断言 `x-opencode-session` 和 `x-session-id` 两个 header 都上了线；`session_header_map`（endpoint.rs:2704）有纯单元测试。缺口只在二进制层的拼接/遍历/失败臂。

**建议**：把 suffix 拼接逻辑抽成纯函数（输入 prefix + 四个 suffix，输出四个完整 session id）做单元测试；或用一个 tempdir Store + 伪造 endpoints 直接测 `apply_session_suffixes` 的 happy path 与 mint-failure 臂。级别：单元。**为什么重要**：decision 84 的正确性目前只在 endpoint 层被钉住，组合根的行为（包括"fatal 而不是静默回退"这一关键决策）没有任何回归保障。

### G3 — `get_or_insert_session_suffix` 竞态收敛臂未测

`tamako-store/src/store.rs:765-807`：已有测试覆盖 mint round-trip、重开持久化、六个 purpose 互不相同的、per-(chat, purpose) 独立（store.rs 测试中 `session_suffix_*` 系列），但 **INSERT OR IGNORE 输掉竞态后 re-SELECT 收敛**的路径没有测试——即两个调用方同时 mint 同一 (chat_id, purpose) 时都必须拿到同一个 suffix。security 审查确认该实现逻辑正确（单事务 SELECT→mint→INSERT OR IGNORE→re-SELECT），但这个保证没有测试钉住。

**建议**：两个 `Store` 实例（或两个连接）对同一 (chat, purpose) 并发/交错调用，断言双方返回相同 suffix 且表内只有一行。级别：tamako-store 单元测试。**为什么重要**：first-write-wins 是 decision 84 的核心语义，一旦未来重构事务边界，没有测试会报警。

### G4 — v12/v13 迁移没有像 v11 那样的前代 shape fixture

v11 迁移测试用手工构造的 v10 shape 数据库（`store.rs:3863-3865` 调 `v10_shaped_db`），走真实 open 路径验证 DROP/recreate + done-journal 重置——这是范本。而 `migration_v12_creates_related_pairs_and_is_idempotent_on_reopen`（store.rs:5018）和 `migration_v13_creates_llm_session_keys_and_is_idempotent_on_reopen`（store.rs:5296）只是用**当前二进制**开新库、断言表存在 + 重开幂等。对纯 additive 的 CREATE TABLE 这风险很低，但与前代 fixture 的做法不对称：如果未来有人把 v12/v13 改成非 additive，现有测试不会失败。

**建议**：补 v11-shaped / v12-shaped 的 fixture 各一个（additive 迁移的 fixture 很便宜）。级别：tamako-store 单元测试。优先级低于 G1–G3。

### G5 — rig 薄壳层（Endpoint*/Rig* 构造与 live 调用路径）系统性未覆盖

`tamako-agent/src/rig_impl.rs` 68.55% lines（`RigExtractor::extract` 零计数）、`tamako-agent/src/caption.rs` 77.15%（`RigCaptionProvider::caption_images` 零计数，只有 live spike 覆盖；`RetryCaptionProvider` 的重试/退避逻辑有覆盖）、`merge_confirm.rs` 83.42%（`EndpointMergeConfirmer::from_endpoint/new/confirm_merge` 零计数）、`summary.rs` 82.18%、`warmup.rs` 82.14%。

**建议**：复用 `the_session_id_header_reaches_the_wire`（endpoint.rs:2781）已经建立的本地 TCP mock server 模式，给 caption/merge-confirm/extract 各加一条 wire 级契约测试（请求体形状 + 响应解析 + 错误映射），不需要真 API。**为什么重要**：caption 的响应解析（`image_url` vs `image_base64` 的 bug 就是这类）只能在 wire 层钉住。

### G6 — 历史遗留弱区（不在本次 diff 范围，记录备查）

- `tamako-memory/src/backend.rs` 28.32% lines：`MemoryBackend` trait 默认方法 + `NoopBackend` 测试替身大多未执行；真正的实现 `lbug_backend.rs` 是 97.97% lines，风险低。
- `tamako/src/persona_watch.rs` 61.11%：`PersonaPathMatcher::new/touches`、`broadcast_preamble`、`evaluate_persona_file`、`spawn_persona_watcher` 零计数；唯一集成测试是 `#[ignore]` 的 fs-watch。`PersonaPathMatcher` 的匹配逻辑是纯的，适合直接单元测试。
- `tamako-store/src/schema.rs` 86.96%：未覆盖的是 `parse_rfc3339`/`format_rfc3339` 的若干错误臂。

### G7 — 覆盖良好、无需行动的区域（确认记录）

- `tamako-core/src/config.rs` 98.82% lines：unknown-key 路径有 `unknown_keys_load_warn_collect_and_apply_nothing`（config.rs:1831）等测试钉住（WARN 的 tracing 输出本身不可测，属正常）。
- `tamako-core/src/merge.rs` 98.78% lines：related_pairs loser-rewrite 在 store 层有 `rewrite_related_pairs_loser_rewrites_dedups_and_drops_self_pairs`，merge 层 apply_one 的 Related 分支有覆盖。JSON 里 merge.rs 的"未覆盖函数"多为 `LbugBackend` 泛型单态化在不同测试 crate 下的产物，非真实缺口。
- `tamako-adapter-teloxide/src/normalize.rs` 98.07%、`tamako-core/src/context.rs` 99.86%（`<media>` 渲染/转义/伪造防护有 1940-2072 的测试组）。

##  brittle test 评估

未发现新代码里有"断言实现细节而非行为"的脆性测试。migration 测试断言确切的列声明顺序和 PK 位置（如 store.rs:5319-5328）是**有意的 schema 钉住**（配合 additive-migration 纪律，schema 形状本身就是契约），context.rs 的渲染测试断言确切转义输出属于行为级断言。唯一值得注意的：`the_session_id_header_reaches_the_wire` 依赖真实 loopback TCP，在沙箱/CI 限制网络的环境里可能脆弱——目前可接受。

## 集成 vs 单元缺口回答（操作员点名的问题）

| 问题 | 答案 |
|---|---|
| 双 header 行为是否 wire-tested？ | **是**。endpoint.rs:2781 真实 TCP wire 测试，两个 header 都断言了。缺口在 main.rs 的拼接层（G2）。|
| v13 迁移是否对 v12 db 文件测过？ | **否**。v12/v13 都是"当前二进制开新库"测试；只有 v11 有前代 shape fixture（G4）。|
| media.rs 富化失败路径？ | **默认测试中零覆盖**，只有 `#[ignore]` live spike（G1）。|
| merge.rs loser-rewrite？ | 覆盖良好（G7）。|
| store.rs session_suffix 冲突路径？ | 收敛臂未测（G3）。|
| config.rs unknown-key 路径？ | 覆盖良好（G7）。|

## 建议落地顺序

1. **G1** media 富化 seam 测试（ScriptedCaption + 假下载源）——钉住 decision 82 的失败收敛不变量，并顺带覆盖 normalize.rs 的 `mentions_bot`/caption-entities 缺口。
2. **G2** apply_session_suffixes 纯函数化 + 单元测试——钉住 decision 84 组合根。
3. **G3** session-suffix 竞态收敛测试。
4. **G5** caption/merge-confirm 的本地 wire 契约测试（复用 endpoint.rs:2781 模式）。
5. **G4** v12/v13 前代 fixture；**G6** persona_watch 纯逻辑单元测试（历史债，可缓）。
