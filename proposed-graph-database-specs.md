# Database Backend Specification for the Tamako Group-Pet Bot

Version: 0.1 Draft
Status: For review
Database: LadybugDB, embedded graph database, fork of KuzuDB

## 1. Purpose

This document specifies the database backend for the long-term memory of the Tamako group-pet bot. The backend stores entities and facts from Telegram group chat messages. The backend supports queries about persons, concepts, and facts in the group.

## 2. Scope

### 2.1 Functions in scope

The backend must:

- Store entities: group members, aliases, concepts.
- Store facts as edges between entities.
- Keep facts that changed. Old facts stay in the database with an invalid time.
- Prevent the merge of two different persons that have the same nickname.
- Operate with open-domain concepts from technical, philosophical, and cultural discussions.
- Give isolation between groups. Each group has one database file.

### 2.2 Functions out of scope

- Queries across groups. The architecture forbids these queries. Refer to Section 4.1.
- Contradiction detection with an LLM. This version keeps the interface only. Refer to Section 7.5.
- Typed node tables. Section 3.2 gives the rationale.

## 3. Design decisions

### 3.1 Selected components

| Decision | Selection | Rationale |
|---|---|---|
| Graph database | LadybugDB, embedded, one file per group | Column storage and CSR adjacency lists. The design load is 100 million edges. The expected load is below 1 percent of this value. |
| External index | Embedded vector index: sqlite-vec or Lance. Optional full-text index. | LadybugDB loads the JSON extension only. The database has no internal full-text or vector search. |
| Redis | Not included. Permitted later as a discardable query cache. | The deterministic identifier gives free exact lookup. Redis cannot do semantic disambiguation. Redis adds a dual-write consistency risk. |

### 3.2 One node table and one edge table

The schema has one `Node` table and one `EDGE` table. The `type` column and the `relationship_name` column give the classification.

The reasons are:

1. The LLM generates concept types and relationship names at runtime. The full set is not known at design time.
2. The `CREATE REL TABLE` statement requires fixed `FROM` and `TO` node tables. Typed node tables with open relationship names cause a combinatorial explosion.
3. At the expected scale, a filter scan on a string column takes milliseconds. The performance cost is negligible.

Replace this design only if one of these conditions is true:

- The node count of one group is more than 10 million.
- A hot query must filter on a field inside the JSON blob, and the field cannot become a column.
- The sidecar vector index becomes too large.

## 4. Storage rules

These rules are mandatory. A violation is a defect.

- R1: Each field used in a `WHERE` clause must be a real column. The JSON blob `properties` can contain display fields only.
- R2: Append is better than update. A change is a new edge plus the invalidation of the old edge. Do not rewrite long text properties.
- R3: Primary keys are short and stable. Primary keys never change. The overflow area for string primary keys grows and is not reclaimed until the table is dropped.
- R4: Updates occur on scalar columns only. Examples are `updated_at`, `invalid_at`, and small enum values.
- R5: Each query enters the graph through an identifier, an alias, or the vector index. Full-graph scan entry is forbidden.

NOTE: An update to a value that exists in the string dictionary is almost free. The engine writes four bytes. An update to a new unique long string appends a full copy. The old bytes are removed at the next checkpoint.

## 5. Deployment and partitioning

### 5.1 One database file per group (mandatory)

- Each `chat_id` has one database file: `{data_root}/{chat_id}/memory.lbug`.
- A query must not touch data of a different group. The memory of a group is private to that group.
- The same person has independent memories in different groups.
- To delete the memory of a group, delete the directory.
- To back up the memory of a group, do a checkpoint, then copy the file.
- LadybugDB permits one writer. The per-group files give write isolation between groups.

### 5.2 Connection management

1. Open the database of a group on first use. Cache the handle in the process. NOTE: In the Rust implementation, the cached handle is the `Database` object. The `lbug` `Connection` borrows the `Database`, so a connection cache would be self-referential. A fresh `Connection` is opened for each operation inside the blocking thread. The cost is negligible.
2. Close the handle after the idle timeout.
3. Wrap the synchronous `Connection.execute()` call in a thread pool. Give an asynchronous interface to callers.
4. Send all user data as `$param` parameters. Permit string interpolation for structural fragments only. Validate interpolated field names against a whitelist.
5. Run `CHECKPOINT` after each batch write. Keep `CHECKPOINT_THRESHOLD` at the default value of 16 MB. Decrease the value during high write load to compact the string dictionary more frequently.
6. Serialize all operations of one group database through one lock per group. Reads and `CHECKPOINT` are included. With lbug 0.18, a read concurrent with a write can crash the process (upstream defect). Refer to `docs/adr-0001-ladybugdb-binding.md`, addendum 2026-08-08.

## 6. Schema

### 6.1 Data definition

```cypher
CREATE NODE TABLE IF NOT EXISTS Node(
    id STRING PRIMARY KEY,
    name STRING,
    type STRING,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    properties STRING
);

CREATE REL TABLE IF NOT EXISTS EDGE(
    FROM Node TO Node,
    relationship_name STRING,
    valid_at TIMESTAMP,
    invalid_at TIMESTAMP,
    edge_text STRING,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    properties STRING
);
```

Column definitions:

| Column | Content |
|---|---|
| `Node.id` | Deterministic UUID5 string. Refer to Section 7. |
| `Node.name` | Canonical name of the entity. |
| `Node.type` | Node type. Closed set. Refer to Section 6.2. |
| `Node.properties` | JSON blob. Display fields only. Rule R1 applies. |
| `EDGE.relationship_name` | System name or open-vocabulary snake_case name. Refer to Section 6.3. |
| `EDGE.valid_at` | Start of fact validity. Usually the message time. |
| `EDGE.invalid_at` | End of fact validity. NULL means the fact is valid now. |
| `EDGE.edge_text` | The natural language text of the fact. One sentence. |
| `EDGE.properties` | JSON blob. Display fields only. |

Two differences from the Cognee schema are intentional:

1. `edge_text`, `valid_at`, and `invalid_at` are real columns, not fields in the JSON blob. The filter `invalid_at IS NULL` occurs in each fact query. Rule R1 applies.
2. The provenance columns and the `GraphMetadata` table are omitted. This version has no pipeline rollback. The `contains` edge to the `MessageBatch` node gives sufficient traceability.

### 6.2 Node types

The `type` column is a closed set:

| Type | Description | Important properties |
|---|---|---|
| `Person` | A group member. The identity is the Telegram user identifier. | `tg_user_id`, `tg_username`, `display_name`, `description` |
| `Alias` | A surface form of a nickname or a concept name. | `surface_form`, `normalized` |
| `Concept` | An open-domain concept. | `description`, `domain` |
| `MessageBatch` | One extraction batch. | `first_msg_id`, `last_msg_id`, `msg_count`, `started_at`, `ended_at` |

### 6.3 System relationship names

These names are reserved. The LLM must not generate these names:

| Name | Direction | Meaning |
|---|---|---|
| `contains` | MessageBatch to Person or Concept | The batch mentions the entity. Provenance only. Not used in reasoning traversal. |
| `known_as` | Person to Alias | A person alias. Isolated per group. |
| `also_known_as` | Concept to Alias | A concept alias. Includes cross-language forms. Example: "GRPO" and the Chinese full name. |
| `is_a` | Concept to Concept | Concept hierarchy. Optional. |
| `supersedes` | Reserved | Marks replacement. For future LLM contradiction detection. Refer to Section 7.5. |

All other relationship names are an open vocabulary. The LLM generates snake_case names. Validate each name with an identifier check before the write. If the name is not valid, use `related_to` and put the original name in the properties.

## 7. Write path

### 7.1 Identifier rules

Each node identifier is a deterministic UUID5 string of 36 bytes. The identifier is stable. Rule R3 applies.

| Node | Identifier rule |
|---|---|
| Person | `uuid5("tg_user:{user_id}")` |
| Alias | `uuid5("alias:{normalized_surface_form}")` |
| Concept | `uuid5("concept:{normalized_canonical_name}")` |
| MessageBatch | `uuid5("batch:{first_msg_id}:{last_msg_id}")` |

Normalization rules: Unicode NFKC, lowercase, no leading or trailing spaces, and one space between words.

CAUTION: Normalization does not merge synonyms across languages. The strings "entropy increase" and its Chinese equivalent get different identifiers. Merge them with an explicit `also_known_as` edge. Refer to step 5 of Section 7.4.

### 7.2 Batch ingestion

1. Collect messages in a sliding window.
2. Start a batch at 20 to 50 messages or after 5 minutes.
3. Keep an overlap of 5 messages between adjacent windows.
4. Format each message with a speaker label: `[{display_name} {HH:MM}] {text}`.
5. If a batch contains only emoji or greetings, skip the extraction. Store the `MessageBatch` skeleton only.
6. Do not let the LLM split long input text. Segmentation is a deterministic engineering task.

### 7.3 Extraction

- The LLM returns a structured `KnowledgeGraph` object: nodes with name, type, description, and edges with source, target, relationship name, description.
- The prompt requires: snake_case relationship names, one specific description per edge, coreference resolution inside the batch, and no knowledge outside the text.
- Before extraction, give the LLM the structured context of the batch: the map from mentions and replies to `tg_user_id` values.

### 7.4 Entity resolution

Do these steps in this sequence for each extracted entity:

1. If the entity is a mention or a reply, get the user identifier from the Telegram API. Bind the entity to the Person node.
2. If the normalized name matches one Alias with one target, bind the entity to that target.
3. Do a vector search on name and description embeddings in the sidecar index:
   - If the top score is 0.92 or more, reuse the node identifier.
   - If the top score is between 0.80 and 0.92, do one LLM confirmation call.
   - If the top score is below 0.80, create a new node.
4. If two or more persons in the group share the alias, use the context: recent speakers and topic relevance. If the ambiguity remains, attach the fact to the Alias node. Do not guess. A wrong binding is worse than a missing fact. A person with no mention binding and no alias match (zero-target person) receives the same treatment: attach the fact to the Alias node with an `attachment: "fallback"` mark. This rate feeds the fallback attachment metric of Section 10.
5. After the binding or the creation, add new surface forms as Alias nodes with alias edges.

NOTE: The vector pre-screen at write time is the primary defense against graph fragmentation. Without this step, one concept becomes many isolated nodes with different surface forms.

### 7.5 Fact validity

The predicate registry has two classes. The registry is a configuration item:

- Single-value predicates: `currently_playing`, `works_at`, `lives_in`, `dating`. One valid edge is permitted for each pair of subject and predicate.
- Multi-value predicates: `likes`, `knows`, `has_pet`. Edges accumulate.

For a new single-value fact, do these steps in one transaction:

1. Set `invalid_at` on each valid edge that has the same subject and the same predicate:

```cypher
MATCH (s:Node)-[r:EDGE]->(o:Node)
WHERE s.id = $subject_id AND r.relationship_name = $rel AND r.invalid_at IS NULL
SET r.invalid_at = $now, r.updated_at = $now
```

2. Merge the new edge with `valid_at` set to the current time.
3. Keep the old edges. They answer queries about the past.

This version does not implement LLM negation detection. The interface is reserved. A message of the form "X does not like Y any more" can then invalidate the related edge and add a `supersedes` mark.

### 7.6 Storage

1. Upsert all nodes and edges of the batch with `UNWIND` and `MERGE` in one transaction.
2. On match, update scalar columns only. Rule R4 applies.
3. If a new description is longer than 1 KB, append a new description edge. Do not rewrite the old value.
4. Run `CHECKPOINT` at the end of the transaction.
5. Write the embeddings of the name and the description of each Person, Alias, and Concept to the sidecar vector index. NOT in the graph transaction (current-state.md decision 66): after the commit, enqueue each new or changed node into the `pending_embeddings` queue of `specs.md` Section 5.2, best-effort. A background worker drains the queue, calls the embeddings endpoint, and writes the vector index. A startup reconciliation pass diffs graph nodes against embedded content hashes and enqueues the missing or stale ones, and drops vector and queue rows whose node no longer exists. Backfill, steady-state repair, and merge-tombstone cleanup all ride this one mechanism.

## 8. Read path

### 8.1 Entry resolution

Do these steps in this sequence:

1. Find mentions and replies in the query. Get the user identifier. Get the Person identifier.
2. Normalize the query term. Find an exact Alias match. Get the target node.
3. Do a vector search. Accept candidates above the threshold.
4. If all steps fail, answer "unknown". A fuzzy full-table scan is forbidden. Rule R5 applies.

### 8.2 Graph expansion rules

- Each traversal must have: a `relationship_name` whitelist, the filter `invalid_at IS NULL`, and a time window on `valid_at` or `created_at`. The default window is 90 days.
- For history queries, remove the `invalid_at IS NULL` filter explicitly.
- The `contains` edge must not occur in a reasoning traversal. Use it for provenance only.
- The expansion limit for one node is 500 edges. Above this limit, truncate by `created_at` descending.
- Mark each node with a degree above 1000 as `hub`. Truncation is mandatory for hub nodes.
- For the query pattern "who discussed X in the group", use the sidecar full-text or vector index on `edge_text` first. Use the graph traversal as a supplement.

### 8.3 Cache

Add a discardable cache only after profiling shows hot queries. Examples are the persona of the bot and frequent memes. Cache invalidation must not change correctness. Redis or an in-process LRU cache are permitted.

## 9. Capacity and performance

Estimate for one active group of 300 members with 2000 messages per day:

| Item | Per day | After one year |
|---|---|---|
| MessageBatch nodes | 80 | 30 000 |
| Person, Concept, Alias nodes after deduplication | 20 to 100 | 10 000 to 40 000 |
| contains edges | 240 to 800 | 100 000 to 300 000 |
| Fact, alias, and is_a edges | 100 to 400 | 50 000 to 150 000 |

The total is 100 000 to 1 000 000 nodes and edges. This is far below the design load of the engine.

Known risks and countermeasures:

| Risk | Countermeasure |
|---|---|
| Supernodes: hot concepts with thousands of edges | Truncation and traversal rules in Section 8.2 |
| Dictionary growth from frequent updates of unique long strings | Rules R2 and R4 |
| Overflow area of string primary keys grows and is not reclaimed | Rule R3 |
| Oversized single values | Keep properties below 10 KB. Never store megabyte values. Store long documents in file storage. Store the reference in the graph. |

## 10. Operations

- Version: pin `ladybug>=0.16.0,<=0.18.2`. Back up the database file before an upgrade. From version 0.18, the storage magic bytes are `LBUG`. Older files have `KUZ` and need the migration tool.
- Backup: copy the single file after a daily checkpoint. The recovery point objective is one checkpoint interval.
- Recovery: if the WAL is damaged, delete the `.wal` file and open the database again.
- Monitoring for each group database:
  - Node count and edge count.
  - Maximum node degree.
  - Checkpoint interval.
  - Batch extraction failure rate.
  - Fallback attachment rate from step 4 of Section 7.4. This rate is the primary quality metric of entity resolution.

## 11. Open items

1. Calibration of the thresholds 0.92 and 0.80 with real group chat data.
2. The initial set of single-value predicates and the process to update the registry.
3. Deduplication strength for repeated facts inside the overlap window. The MERGE operation is idempotent. Decide if more detection is necessary.
4. Introduction of the `is_a` concept hierarchy. Decide the maintenance process: LLM bootstrap or manual review.
