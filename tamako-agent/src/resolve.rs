//! Entity resolution and MemoryBatch construction. Section 7.4 of the
//! database spec, Phase 1 scope (dev-roadmap.md Section 3 item 4): steps
//! 1, 2, and 4 only — mention binding, exact alias match, and the
//! ambiguity fallback to the Alias node. No vector search, no LLM
//! confirmation (Phase 2).
//!
//! Design decision beyond the spec text: a person with NO mention
//! binding and NO alias target also attaches to its Alias node (the same
//! fallback as step 4). Reason: a person without a tg_user_id binding
//! has no deterministic person identifier, and a wrong binding is worse
//! than a missing fact (Section 7.4 step 4). The fallback attachment
//! rate is the primary resolution quality metric (specs.md Section 12),
//! so every fallback Alias node carries the marker
//! `"attachment": "fallback"` in its properties.

use std::collections::{HashMap, HashSet};

use tamako_memory::identifiers::{alias_id, concept_id, normalize, person_id};
use tamako_memory::{MemoryBackend, MemoryBatch, MemoryEdge, MemoryNode, NodeType};
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;

use crate::extract::{AgentError, MentionBinding};
use crate::graph::{ExtractedNodeType, KnowledgeGraph};
use crate::validate::{RelationshipName, FALLBACK_RELATIONSHIP_NAME};

/// The edge properties key of the fallback relationship name marker.
/// Section 6.3: the original name goes into the edge properties.
const ORIGINAL_RELATIONSHIP_NAME_KEY: &str = "original_relationship_name";

/// How one extracted node was bound to a graph node id.
struct ResolvedEntity {
    /// The graph node id the entity's edges attach to.
    node_id: String,
    node_type: ExtractedNodeType,
    /// The tg_user_id of a step-1 mention binding. None for every other
    /// resolution path.
    tg_user_id: Option<String>,
    /// True when the entity attached to its own Alias node (Section 7.4
    /// step 4 fallback). No alias edge and no contains edge is written
    /// for such an entity.
    attached_to_alias: bool,
    /// True when the entity bound to an existing node through an exact
    /// alias match (step 2). Such a node update carries NO properties:
    /// the MERGE coalesce keeps the stored identity blob (tg_user_id,
    /// display_name). An overwrite would drop the identity data.
    bound_via_alias: bool,
}

/// RFC 3339 or the debug form as a last resort. The Rfc3339 format only
/// fails for years outside 0..=9999; log timestamps never reach that.
fn rfc3339(timestamp: OffsetDateTime) -> String {
    timestamp
        .format(&Rfc3339)
        .unwrap_or_else(|_| format!("{timestamp:?}"))
}

/// The MessageBatch node of the batch. Section 6.2. Shared by the full
/// resolution and the skeleton path of the pipeline (Section 7.2
/// rule 5). The deterministic batch id makes the node idempotent under
/// MERGE.
pub(crate) fn message_batch_node(
    batch_id: &str,
    first_msg_id: i64,
    last_msg_id: i64,
    msg_count: u32,
    started_at: OffsetDateTime,
    ended_at: OffsetDateTime,
    now: OffsetDateTime,
) -> MemoryNode {
    let properties = serde_json::json!({
        "first_msg_id": first_msg_id,
        "last_msg_id": last_msg_id,
        "msg_count": msg_count,
        "started_at": rfc3339(started_at),
        "ended_at": rfc3339(ended_at),
    });
    MemoryNode {
        id: batch_id.to_string(),
        name: batch_id.to_string(),
        node_type: NodeType::MessageBatch,
        created_at: now,
        updated_at: now,
        properties: Some(properties.to_string()),
    }
}

/// Resolves every extracted node to a graph node id and builds the
/// MemoryBatch content (nodes, fact edges, alias edges, contains edges,
/// the MessageBatch node). Section 7.4 (steps 1, 2, 4 only — no vector
/// search, no LLM confirmation, Phase 2), Section 7.5 Phase 1 form
/// (multi-value: valid_at = batch end, invalid_at = NULL), Section 6.3
/// (alias edges, contains edges).
///
/// `validated_names` is parallel to `graph.edges` (the pipeline
/// post-validates every name first; `validate.rs`).
#[allow(clippy::too_many_arguments)]
pub async fn resolve_batch<M: MemoryBackend>(
    memory: &M,
    chat_id: &str,
    graph: &KnowledgeGraph,
    validated_names: &[RelationshipName],
    mention_map: &[MentionBinding],
    batch_id: &str,
    first_msg_id: i64,
    last_msg_id: i64,
    batch_end: OffsetDateTime,
    msg_count: u32,
    started_at: OffsetDateTime,
) -> Result<MemoryBatch, AgentError> {
    let now = OffsetDateTime::now_utc();
    let mut nodes: Vec<MemoryNode> = Vec::new();
    let mut edges: Vec<MemoryEdge> = Vec::new();
    // Node dedupe by id: the same node can be reachable twice (example:
    // a bound target plus the alias of the same name).
    let mut seen_node_ids: HashSet<String> = HashSet::new();
    let mut resolved: HashMap<String, ResolvedEntity> = HashMap::new();
    // Contains targets: resolved Person/Concept nodes (id, display name),
    // deduped by id. Alias-attached entities are excluded (Section 6.3:
    // contains targets only Person/Concept).
    let mut contains_targets: Vec<(String, String)> = Vec::new();
    let mut seen_contains: HashSet<String> = HashSet::new();

    let mut push_node = |node: MemoryNode| {
        if seen_node_ids.insert(node.id.clone()) {
            nodes.push(node);
        }
    };

    for extracted in &graph.nodes {
        let entity = resolve_one(memory, chat_id, extracted, mention_map).await?;
        if !entity.attached_to_alias && seen_contains.insert(entity.node_id.clone()) {
            contains_targets.push((entity.node_id.clone(), extracted.name.clone()));
        }
        resolved.insert(extracted.name.clone(), entity);
    }

    // Section 7.4 step 5 (M1 scope): for every Person/Concept entity,
    // the surface-form Alias node plus the alias edge. Skipped when the
    // entity resolved to the alias itself (no self-edge).
    for extracted in &graph.nodes {
        let entity = &resolved[&extracted.name];
        if entity.attached_to_alias {
            continue;
        }
        let surface_alias_id = alias_id(&extracted.name);
        push_node(alias_node(&extracted.name, now));
        let relationship_name = match extracted.node_type {
            // Section 6.3: Person -> Alias is known_as, Concept -> Alias
            // is also_known_as.
            ExtractedNodeType::Person => "known_as",
            ExtractedNodeType::Concept => "also_known_as",
        };
        edges.push(MemoryEdge {
            source_id: entity.node_id.clone(),
            target_id: surface_alias_id,
            relationship_name: relationship_name.to_string(),
            valid_at: batch_end,
            invalid_at: None,
            edge_text: format!(
                "{} is a surface form of {}.",
                extracted.name, extracted.name
            ),
            created_at: now,
            updated_at: now,
            properties: None,
        });
    }

    // The nodes of the graph (after alias nodes so an entity node that
    // shares an id with an alias node is not shadowed).
    for extracted in &graph.nodes {
        let entity = &resolved[&extracted.name];
        let node = entity_node(extracted, entity, now);
        push_node(node);
    }

    // Fact edges. Section 6.3: fallback names become related_to with the
    // original name in the properties.
    if validated_names.len() != graph.edges.len() {
        // The pipeline guarantees a parallel slice. A mismatch is a
        // caller bug; resolve the parallel prefix rather than panic.
        tracing::warn!(
            validated = validated_names.len(),
            edges = graph.edges.len(),
            "validated relationship names are not parallel to the extracted edges"
        );
    }
    for (edge, validated) in graph.edges.iter().zip(validated_names.iter()) {
        let (source, target) = match (resolved.get(&edge.source), resolved.get(&edge.target)) {
            (Some(source), Some(target)) => (source, target),
            _ => {
                // The LLM hallucinated an endpoint. Drop the edge; do
                // not create phantom nodes.
                tracing::warn!(
                    source = %edge.source,
                    target = %edge.target,
                    "dropping an edge with an endpoint that is not an extracted node"
                );
                continue;
            }
        };
        let (relationship_name, properties) = match validated {
            RelationshipName::Valid(name) => (name.clone(), None),
            RelationshipName::Fallback { original } => (
                FALLBACK_RELATIONSHIP_NAME.to_string(),
                Some(serde_json::json!({ ORIGINAL_RELATIONSHIP_NAME_KEY: original }).to_string()),
            ),
        };
        edges.push(MemoryEdge {
            source_id: source.node_id.clone(),
            target_id: target.node_id.clone(),
            relationship_name,
            // Section 7.5, Phase 1 form (dev-roadmap.md Section 3
            // item 5): every predicate is multi-value; invalid_at stays
            // NULL.
            valid_at: batch_end,
            invalid_at: None,
            edge_text: edge.description.clone(),
            created_at: now,
            updated_at: now,
            properties,
        });
    }

    // Contains edges (Section 6.3, provenance): the batch node to every
    // resolved Person/Concept node the batch mentions.
    for (target_id, name) in &contains_targets {
        edges.push(MemoryEdge {
            source_id: batch_id.to_string(),
            target_id: target_id.clone(),
            relationship_name: "contains".to_string(),
            valid_at: batch_end,
            invalid_at: None,
            edge_text: format!("batch {first_msg_id}-{last_msg_id} mentions {name}"),
            created_at: now,
            updated_at: now,
            properties: None,
        });
    }

    // The MessageBatch node itself. upsert_batch also MERGEs the
    // skeleton by id; this node gives it the real properties of
    // Section 6.2. Idempotent.
    push_node(message_batch_node(
        batch_id,
        first_msg_id,
        last_msg_id,
        msg_count,
        started_at,
        batch_end,
        now,
    ));

    Ok(MemoryBatch {
        batch_id: batch_id.to_string(),
        nodes,
        edges,
    })
}

/// Resolves one extracted node to its graph node id. Section 7.4.
async fn resolve_one<M: MemoryBackend>(
    memory: &M,
    chat_id: &str,
    extracted: &crate::graph::ExtractedNode,
    mention_map: &[MentionBinding],
) -> Result<ResolvedEntity, AgentError> {
    match extracted.node_type {
        ExtractedNodeType::Person => resolve_person(memory, chat_id, extracted, mention_map).await,
        ExtractedNodeType::Concept => resolve_concept(memory, chat_id, extracted).await,
    }
}

/// Person resolution. Section 7.4 steps 1, 2, 4.
async fn resolve_person<M: MemoryBackend>(
    memory: &M,
    chat_id: &str,
    extracted: &crate::graph::ExtractedNode,
    mention_map: &[MentionBinding],
) -> Result<ResolvedEntity, AgentError> {
    // Step 1: mention/reply binding. Both sides are normalized
    // (Section 7.1) before the comparison.
    let normalized_name = normalize(&extracted.name);
    if let Some(binding) = mention_map
        .iter()
        .find(|binding| normalize(&binding.display_name) == normalized_name)
    {
        return Ok(ResolvedEntity {
            node_id: person_id(&binding.tg_user_id),
            node_type: ExtractedNodeType::Person,
            tg_user_id: Some(binding.tg_user_id.clone()),
            attached_to_alias: false,
            bound_via_alias: false,
        });
    }

    // Step 2: exact alias match. Enter the graph through the
    // deterministic alias identifier (Rule R5).
    let surface_alias_id = alias_id(&extracted.name);
    let targets = memory.alias_targets(chat_id, &surface_alias_id).await?;
    if let [target] = targets.as_slice() {
        if target.node_type == NodeType::Person {
            // Bind to the existing node id; MERGE updates name and
            // description.
            return Ok(ResolvedEntity {
                node_id: target.node_id.clone(),
                node_type: ExtractedNodeType::Person,
                tg_user_id: None,
                attached_to_alias: false,
                bound_via_alias: true,
            });
        }
    }

    // Step 4: two or more targets, one target of a non-matching type,
    // or (design decision, module docs) no target at all: DO NOT GUESS.
    // The entity's edges attach to the Alias node.
    Ok(ResolvedEntity {
        node_id: surface_alias_id,
        node_type: ExtractedNodeType::Person,
        tg_user_id: None,
        attached_to_alias: true,
        bound_via_alias: false,
    })
}

/// Concept resolution. The deterministic identifier covers repeats
/// (fragmentation accepted in Phase 1, dev-roadmap.md Section 3). The
/// alias pre-binding is a cheap dedup only.
async fn resolve_concept<M: MemoryBackend>(
    memory: &M,
    chat_id: &str,
    extracted: &crate::graph::ExtractedNode,
) -> Result<ResolvedEntity, AgentError> {
    let surface_alias_id = alias_id(&extracted.name);
    let targets = memory.alias_targets(chat_id, &surface_alias_id).await?;
    if let [target] = targets.as_slice() {
        if target.node_type == NodeType::Concept {
            return Ok(ResolvedEntity {
                node_id: target.node_id.clone(),
                node_type: ExtractedNodeType::Concept,
                tg_user_id: None,
                attached_to_alias: false,
                bound_via_alias: true,
            });
        }
    }
    if targets.len() >= 2 {
        // An ambiguous concept alias: fall back to the deterministic id.
        tracing::debug!(
            name = %extracted.name,
            targets = targets.len(),
            "concept alias has several targets; using the deterministic concept id"
        );
    }
    Ok(ResolvedEntity {
        node_id: concept_id(&extracted.name),
        node_type: ExtractedNodeType::Concept,
        tg_user_id: None,
        attached_to_alias: false,
        bound_via_alias: false,
    })
}

/// The surface-form Alias node. Section 6.2.
fn alias_node(surface_form: &str, now: OffsetDateTime) -> MemoryNode {
    let properties = serde_json::json!({
        "surface_form": surface_form,
        "normalized": normalize(surface_form),
    });
    MemoryNode {
        id: alias_id(surface_form),
        name: surface_form.to_string(),
        node_type: NodeType::Alias,
        created_at: now,
        updated_at: now,
        properties: Some(properties.to_string()),
    }
}

/// The graph node of one extracted entity after resolution.
fn entity_node(
    extracted: &crate::graph::ExtractedNode,
    entity: &ResolvedEntity,
    now: OffsetDateTime,
) -> MemoryNode {
    if entity.attached_to_alias {
        // The fallback Alias node doubles as the entity node. The
        // attachment marker feeds the fallback attachment rate metric
        // (specs.md Section 12, database spec Section 10).
        let properties = serde_json::json!({
            "surface_form": extracted.name,
            "normalized": normalize(&extracted.name),
            "attachment": "fallback",
        });
        return MemoryNode {
            id: entity.node_id.clone(),
            name: extracted.name.clone(),
            node_type: NodeType::Alias,
            created_at: now,
            updated_at: now,
            properties: Some(properties.to_string()),
        };
    }
    match entity.node_type {
        ExtractedNodeType::Person => {
            let properties = match &entity.tg_user_id {
                // Section 7.4 step 1: the mention binding carries the
                // tg_user_id into the node properties.
                Some(tg_user_id) => serde_json::json!({
                    "tg_user_id": tg_user_id,
                    "display_name": extracted.name,
                    "description": extracted.description,
                })
                .to_string(),
                // An alias-bound person (step 2) keeps its stored
                // identity properties through the MERGE coalesce: a
                // None parameter never overwrites the stored blob. An
                // overwrite would drop tg_user_id and display_name.
                None => {
                    return MemoryNode {
                        id: entity.node_id.clone(),
                        name: extracted.name.clone(),
                        node_type: NodeType::Person,
                        created_at: now,
                        updated_at: now,
                        properties: None,
                    }
                }
            };
            MemoryNode {
                id: entity.node_id.clone(),
                name: extracted.name.clone(),
                node_type: NodeType::Person,
                created_at: now,
                updated_at: now,
                properties: Some(properties),
            }
        }
        ExtractedNodeType::Concept => MemoryNode {
            id: entity.node_id.clone(),
            name: extracted.name.clone(),
            node_type: NodeType::Concept,
            created_at: now,
            updated_at: now,
            // An alias-bound concept keeps its stored properties through
            // the MERGE coalesce, like the alias-bound person.
            properties: if entity.bound_via_alias {
                None
            } else {
                Some(serde_json::json!({ "description": extracted.description }).to_string())
            },
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::extract::BindingSource;
    use crate::graph::{ExtractedEdge, ExtractedNode};
    use crate::validate::validate_relationship_name;
    use tamako_memory::identifiers::{alias_id, concept_id, person_id};
    use tamako_memory::LbugBackend;
    use time::macros::datetime;

    const STARTED: OffsetDateTime = datetime!(2026-08-07 10:00 UTC);
    const BATCH_END: OffsetDateTime = datetime!(2026-08-07 10:05 UTC);
    const CHAT: &str = "resolve_test";

    fn node(name: &str, node_type: ExtractedNodeType) -> ExtractedNode {
        ExtractedNode {
            name: name.to_string(),
            node_type,
            description: format!("A description of {name}."),
        }
    }

    fn graph(nodes: Vec<ExtractedNode>, edges: Vec<ExtractedEdge>) -> KnowledgeGraph {
        KnowledgeGraph { nodes, edges }
    }

    async fn backend() -> (tempfile::TempDir, LbugBackend) {
        let dir = tempfile::tempdir().expect("tempdir");
        let backend = LbugBackend::new(dir.path());
        backend.ensure_schema(CHAT).await.expect("schema");
        (dir, backend)
    }

    async fn resolve<M: MemoryBackend>(
        memory: &M,
        extracted: &KnowledgeGraph,
        mention_map: &[MentionBinding],
    ) -> MemoryBatch {
        let validated: Vec<RelationshipName> = extracted
            .edges
            .iter()
            .map(|edge| validate_relationship_name(&edge.relationship_name))
            .collect();
        resolve_batch(
            memory,
            CHAT,
            extracted,
            &validated,
            mention_map,
            "batch-resolve-test",
            1,
            10,
            BATCH_END,
            10,
            STARTED,
        )
        .await
        .expect("resolve")
    }

    fn find_node<'a>(batch: &'a MemoryBatch, id: &str) -> Option<&'a MemoryNode> {
        batch.nodes.iter().find(|node| node.id == id)
    }

    fn edges_named<'a>(batch: &'a MemoryBatch, name: &str) -> Vec<&'a MemoryEdge> {
        batch
            .edges
            .iter()
            .filter(|edge| edge.relationship_name == name)
            .collect()
    }

    fn mention(display_name: &str, tg_user_id: &str) -> MentionBinding {
        MentionBinding {
            display_name: display_name.to_string(),
            tg_user_id: tg_user_id.to_string(),
            source: BindingSource::Sender,
        }
    }

    /// Seeds a Person plus an Alias plus the alias edge (Section 7.4
    /// step 2 test data).
    async fn seed_person_alias<M: MemoryBackend>(
        memory: &M,
        tg_user_id: &str,
        name: &str,
        surface_form: &str,
    ) {
        let now = BATCH_END;
        let person = MemoryNode {
            id: person_id(tg_user_id),
            name: name.to_string(),
            node_type: NodeType::Person,
            created_at: now,
            updated_at: now,
            properties: None,
        };
        let alias = MemoryNode {
            id: alias_id(surface_form),
            name: surface_form.to_string(),
            node_type: NodeType::Alias,
            created_at: now,
            updated_at: now,
            properties: None,
        };
        let edge = MemoryEdge {
            source_id: person.id.clone(),
            target_id: alias.id.clone(),
            relationship_name: "known_as".to_string(),
            valid_at: now,
            invalid_at: None,
            edge_text: format!("{surface_form} is a surface form of {name}."),
            created_at: now,
            updated_at: now,
            properties: None,
        };
        let batch = MemoryBatch {
            batch_id: "seed".to_string(),
            nodes: vec![person, alias],
            edges: vec![edge],
        };
        memory.upsert_batch(CHAT, &batch).await.expect("seed");
    }

    #[tokio::test]
    async fn a_mention_binding_binds_to_the_person_id() {
        // Section 7.4 step 1.
        let (_dir, memory) = backend().await;
        let extracted = graph(vec![node("Alice", ExtractedNodeType::Person)], vec![]);
        let batch = resolve(&memory, &extracted, &[mention("alice", "1001")]).await;

        let person = find_node(&batch, &person_id("1001")).expect("person node");
        assert_eq!(person.node_type, NodeType::Person);
        let properties = person.properties.as_deref().expect("properties");
        assert!(properties.contains("\"tg_user_id\":\"1001\""));
        assert!(properties.contains("\"display_name\":\"Alice\""));

        // Section 7.4 step 5: the surface-form alias plus known_as.
        let alias = find_node(&batch, &alias_id("Alice")).expect("alias node");
        assert_eq!(alias.node_type, NodeType::Alias);
        let known_as = edges_named(&batch, "known_as");
        assert_eq!(known_as.len(), 1);
        assert_eq!(known_as[0].source_id, person_id("1001"));
        assert_eq!(known_as[0].target_id, alias_id("Alice"));

        // Section 6.3: the batch node contains the person.
        let contains = edges_named(&batch, "contains");
        assert_eq!(contains.len(), 1);
        assert_eq!(contains[0].source_id, "batch-resolve-test");
        assert_eq!(contains[0].target_id, person_id("1001"));
        // Section 7.5 Phase 1 form: valid_at set, invalid_at NULL.
        assert_eq!(contains[0].valid_at, BATCH_END);
        assert_eq!(contains[0].invalid_at, None);
    }

    #[tokio::test]
    async fn an_exact_alias_binds_to_the_existing_person() {
        // Section 7.4 step 2: one alias, one target, matching type.
        let (_dir, memory) = backend().await;
        seed_person_alias(&memory, "1001", "Tama", "tama").await;
        let extracted = graph(vec![node("Tama", ExtractedNodeType::Person)], vec![]);
        let batch = resolve(&memory, &extracted, &[]).await;

        let person = find_node(&batch, &person_id("1001")).expect("bound person");
        assert_eq!(person.node_type, NodeType::Person);
        // No fallback attachment: the entity bound to the real person.
        assert!(
            find_node(&batch, &alias_id("Tama")).map(|alias| alias
                .properties
                .as_deref()
                .unwrap_or_default()
                .contains("fallback"))
                == Some(false)
        );
    }

    #[tokio::test]
    async fn an_ambiguous_alias_attaches_the_fact_to_the_alias_node() {
        // Section 7.4 step 4: two persons share one alias. Do not guess.
        let (_dir, memory) = backend().await;
        seed_person_alias(&memory, "1001", "Tama One", "tama").await;
        seed_person_alias(&memory, "2002", "Tama Two", "tama").await;
        let extracted = graph(
            vec![
                node("tama", ExtractedNodeType::Person),
                node("GRPO", ExtractedNodeType::Concept),
            ],
            vec![ExtractedEdge {
                source: "tama".to_string(),
                target: "GRPO".to_string(),
                relationship_name: "likes".to_string(),
                description: "tama said tama likes GRPO.".to_string(),
            }],
        );
        let batch = resolve(&memory, &extracted, &[]).await;

        // The entity attached to the alias node, with the fallback
        // marker (specs.md Section 12: fallback attachment rate).
        let alias = find_node(&batch, &alias_id("tama")).expect("alias node");
        assert_eq!(alias.node_type, NodeType::Alias);
        assert!(alias
            .properties
            .as_deref()
            .unwrap_or_default()
            .contains("\"attachment\":\"fallback\""));
        // Both person ids stay untouched: neither is re-created.
        assert!(find_node(&batch, &person_id("1001")).is_none());
        assert!(find_node(&batch, &person_id("2002")).is_none());
        // The fact edge attaches to the alias node.
        let likes = edges_named(&batch, "likes");
        assert_eq!(likes.len(), 1);
        assert_eq!(likes[0].source_id, alias_id("tama"));
        // Section 6.3: contains targets only Person/Concept. The
        // alias-attached entity gets no contains edge; the concept does.
        let contains = edges_named(&batch, "contains");
        assert_eq!(contains.len(), 1);
        assert_eq!(contains[0].target_id, concept_id("GRPO"));
    }

    #[tokio::test]
    async fn an_unresolved_person_attaches_to_a_new_alias_node() {
        // No mention, no alias: the same step-4 fallback (module docs).
        let (_dir, memory) = backend().await;
        let extracted = graph(vec![node("Stranger", ExtractedNodeType::Person)], vec![]);
        let batch = resolve(&memory, &extracted, &[]).await;

        let alias = find_node(&batch, &alias_id("Stranger")).expect("alias node");
        assert_eq!(alias.node_type, NodeType::Alias);
        assert!(alias
            .properties
            .as_deref()
            .unwrap_or_default()
            .contains("\"attachment\":\"fallback\""));
        // No self alias edge and no contains edge for the fallback.
        assert!(edges_named(&batch, "known_as").is_empty());
        assert!(edges_named(&batch, "contains").is_empty());
    }

    #[tokio::test]
    async fn a_concept_uses_the_deterministic_concept_id() {
        // dev-roadmap.md Section 3: fragmentation accepted in Phase 1.
        let (_dir, memory) = backend().await;
        let extracted = graph(vec![node("GRPO", ExtractedNodeType::Concept)], vec![]);
        let batch = resolve(&memory, &extracted, &[]).await;

        let concept = find_node(&batch, &concept_id("GRPO")).expect("concept node");
        assert_eq!(concept.node_type, NodeType::Concept);
        let also_known_as = edges_named(&batch, "also_known_as");
        assert_eq!(also_known_as.len(), 1);
        assert_eq!(also_known_as[0].source_id, concept_id("GRPO"));
        assert_eq!(also_known_as[0].target_id, alias_id("GRPO"));
        assert_eq!(edges_named(&batch, "contains").len(), 1);
    }

    #[tokio::test]
    async fn a_single_concept_alias_target_is_reused() {
        // Cheap dedup: one Concept target of the surface-form alias
        // wins over the deterministic id.
        let (_dir, memory) = backend().await;
        let long_form_id = concept_id("group relative policy optimization");
        let now = BATCH_END;
        let seed = MemoryBatch {
            batch_id: "seed-concept".to_string(),
            nodes: vec![
                MemoryNode {
                    id: long_form_id.clone(),
                    name: "group relative policy optimization".to_string(),
                    node_type: NodeType::Concept,
                    created_at: now,
                    updated_at: now,
                    properties: None,
                },
                MemoryNode {
                    id: alias_id("GRPO"),
                    name: "GRPO".to_string(),
                    node_type: NodeType::Alias,
                    created_at: now,
                    updated_at: now,
                    properties: None,
                },
            ],
            edges: vec![MemoryEdge {
                source_id: long_form_id.clone(),
                target_id: alias_id("GRPO"),
                relationship_name: "also_known_as".to_string(),
                valid_at: now,
                invalid_at: None,
                edge_text: "GRPO is a surface form.".to_string(),
                created_at: now,
                updated_at: now,
                properties: None,
            }],
        };
        memory.upsert_batch(CHAT, &seed).await.expect("seed");

        let extracted = graph(vec![node("GRPO", ExtractedNodeType::Concept)], vec![]);
        let batch = resolve(&memory, &extracted, &[]).await;
        assert!(find_node(&batch, &long_form_id).is_some());
        // The deterministic id of the surface form is NOT created.
        assert_ne!(long_form_id, concept_id("GRPO"));
        assert!(find_node(&batch, &concept_id("GRPO")).is_none());
    }

    #[tokio::test]
    async fn an_invalid_relationship_name_falls_back_with_the_original() {
        // Section 6.3: related_to plus the original name in properties.
        let (_dir, memory) = backend().await;
        let extracted = graph(
            vec![
                node("Alice", ExtractedNodeType::Person),
                node("GRPO", ExtractedNodeType::Concept),
            ],
            vec![ExtractedEdge {
                source: "Alice".to_string(),
                target: "GRPO".to_string(),
                relationship_name: "Likes".to_string(),
                description: "Alice likes GRPO.".to_string(),
            }],
        );
        let batch = resolve(&memory, &extracted, &[mention("Alice", "1001")]).await;
        let related = edges_named(&batch, "related_to");
        assert_eq!(related.len(), 1);
        assert_eq!(
            related[0].properties.as_deref(),
            Some("{\"original_relationship_name\":\"Likes\"}")
        );
        assert_eq!(related[0].edge_text, "Alice likes GRPO.");
    }

    #[tokio::test]
    async fn a_reserved_name_from_the_llm_is_rejected_to_related_to() {
        // Section 6.3: never trust the prompt.
        let (_dir, memory) = backend().await;
        let extracted = graph(
            vec![
                node("Alice", ExtractedNodeType::Person),
                node("GRPO", ExtractedNodeType::Concept),
            ],
            vec![ExtractedEdge {
                source: "Alice".to_string(),
                target: "GRPO".to_string(),
                relationship_name: "contains".to_string(),
                description: "Hallucinated system edge.".to_string(),
            }],
        );
        let batch = resolve(&memory, &extracted, &[mention("Alice", "1001")]).await;
        // The LLM-generated "contains" became related_to. The only real
        // contains edges are the provenance edges from the batch node.
        let contains = edges_named(&batch, "contains");
        assert!(contains
            .iter()
            .all(|edge| edge.source_id == "batch-resolve-test"));
        let related = edges_named(&batch, "related_to");
        assert_eq!(related.len(), 1);
        assert_eq!(
            related[0].properties.as_deref(),
            Some("{\"original_relationship_name\":\"contains\"}")
        );
    }

    #[tokio::test]
    async fn an_edge_with_an_unknown_endpoint_is_dropped() {
        // The LLM hallucinated an endpoint; do not create phantom nodes.
        let (_dir, memory) = backend().await;
        let extracted = graph(
            vec![node("Alice", ExtractedNodeType::Person)],
            vec![ExtractedEdge {
                source: "Alice".to_string(),
                target: "Ghost".to_string(),
                relationship_name: "knows".to_string(),
                description: "Alice knows Ghost.".to_string(),
            }],
        );
        let batch = resolve(&memory, &extracted, &[mention("Alice", "1001")]).await;
        assert!(edges_named(&batch, "knows").is_empty());
        assert!(find_node(&batch, &concept_id("Ghost")).is_none());
        assert!(find_node(&batch, &alias_id("Ghost")).is_none());
    }

    #[tokio::test]
    async fn the_batch_contains_the_message_batch_node_of_section_6_2() {
        let (_dir, memory) = backend().await;
        let extracted = graph(vec![node("GRPO", ExtractedNodeType::Concept)], vec![]);
        let batch = resolve(&memory, &extracted, &[]).await;
        let batch_node = find_node(&batch, "batch-resolve-test").expect("batch node");
        assert_eq!(batch_node.node_type, NodeType::MessageBatch);
        let properties = batch_node.properties.as_deref().expect("properties");
        assert!(properties.contains("\"first_msg_id\":1"));
        assert!(properties.contains("\"last_msg_id\":10"));
        assert!(properties.contains("\"msg_count\":10"));
        assert!(properties.contains("2026-08-07T10:00:00Z"));
        assert!(properties.contains("2026-08-07T10:05:00Z"));
    }

    #[tokio::test]
    async fn an_alias_bound_person_keeps_its_stored_identity_properties() {
        // Regression test. Section 7.4 step 2: a person bound through an
        // exact alias match must NOT overwrite the stored identity blob
        // (tg_user_id, display_name). The MemoryNode carries
        // properties = None and the MERGE coalesce keeps the stored
        // value (Rule R1: tg_user_id is an important property,
        // Section 6.2).
        let (_dir, memory) = backend().await;
        let now = BATCH_END;
        let person = MemoryNode {
            id: person_id("1001"),
            name: "Tama".to_string(),
            node_type: NodeType::Person,
            created_at: now,
            updated_at: now,
            properties: Some(
                serde_json::json!({"tg_user_id": "1001", "display_name": "Tama"}).to_string(),
            ),
        };
        let alias = MemoryNode {
            id: alias_id("tama"),
            name: "tama".to_string(),
            node_type: NodeType::Alias,
            created_at: now,
            updated_at: now,
            properties: None,
        };
        let known_as = MemoryEdge {
            source_id: person.id.clone(),
            target_id: alias.id.clone(),
            relationship_name: "known_as".to_string(),
            valid_at: now,
            invalid_at: None,
            edge_text: "tama is a surface form of Tama.".to_string(),
            created_at: now,
            updated_at: now,
            properties: None,
        };
        let seed = MemoryBatch {
            batch_id: "seed-identity".to_string(),
            nodes: vec![person, alias],
            edges: vec![known_as],
        };
        memory.upsert_batch(CHAT, &seed).await.expect("seed");

        // A later batch mentions "tama" without a mention binding. The
        // entity binds through the alias. Its node update must carry no
        // properties.
        let extracted = graph(vec![node("tama", ExtractedNodeType::Person)], vec![]);
        let batch = resolve(&memory, &extracted, &[]).await;
        let bound = find_node(&batch, &person_id("1001")).expect("bound person");
        assert_eq!(bound.properties, None);
        memory.upsert_batch(CHAT, &batch).await.expect("upsert");

        // The stored identity blob survives.
        let rows = memory
            .query_rows(
                CHAT,
                "MATCH (n:Node) WHERE n.type = 'Person' RETURN n.properties",
            )
            .await
            .expect("query");
        assert_eq!(rows.len(), 1);
        assert!(rows[0][0].contains("\"tg_user_id\":\"1001\""));
        assert!(rows[0][0].contains("\"display_name\":\"Tama\""));
    }
}
