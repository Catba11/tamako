//! The LLM-facing extraction types of Section 7.3 of the database spec.
//!
//! The LLM produces exactly this shape. The rig output schema is built
//! from these types (`schemars::schema_for!(KnowledgeGraph)`), so the doc
//! comments are part of the prompt: schemars turns them into schema
//! descriptions on the Anthropic structured-output path.

/// The structured extraction result. Section 7.3 of the database spec.
/// The LLM produces exactly this shape (rig output_schema).
#[derive(
    Debug, Clone, Default, PartialEq, serde::Serialize, serde::Deserialize, schemars::JsonSchema,
)]
pub struct KnowledgeGraph {
    /// The entities of the batch. Constrained to Person and Concept.
    pub nodes: Vec<ExtractedNode>,
    /// The facts of the batch. Endpoints reference node names of this
    /// batch.
    pub edges: Vec<ExtractedEdge>,
}

/// One extracted entity.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, schemars::JsonSchema)]
pub struct ExtractedNode {
    /// Canonical name of the entity as it appears in the batch. For a
    /// mentioned or replied-to person, the display name of the
    /// mention/reply map entry.
    pub name: String,
    /// Constrained to Person and Concept. Alias and MessageBatch are
    /// system-created (Section 6.2; M1 scope).
    pub node_type: ExtractedNodeType,
    /// One specific description grounded in the batch text.
    pub description: String,
}

/// The node types the LLM may produce. Section 6.2: Alias and
/// MessageBatch nodes are system-created, never LLM-created.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize, schemars::JsonSchema,
)]
pub enum ExtractedNodeType {
    /// A group member.
    Person,
    /// An open-domain concept.
    Concept,
}

/// One extracted fact.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, schemars::JsonSchema)]
pub struct ExtractedEdge {
    /// The `name` of the source node of this batch.
    pub source: String,
    /// The `name` of the target node of this batch.
    pub target: String,
    /// snake_case relationship name (lowercase letters, digits,
    /// underscores; starts with a letter). The reserved system names
    /// contains, known_as, also_known_as, is_a, and supersedes must not
    /// be used (Section 6.3). Never trust the prompt: the name is
    /// validated again in Rust.
    pub relationship_name: String,
    /// One specific description of this fact, grounded in the text.
    pub description: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_schema_serializes_the_constrained_node_type() {
        // The schema is part of the prompt on the Anthropic
        // structured-output path. The enum must appear as its serde
        // representation ("Person"/"Concept").
        let schema = schemars::schema_for!(KnowledgeGraph);
        let json = serde_json::to_string(&schema).expect("schema to json");
        assert!(json.contains("\"Person\""));
        assert!(json.contains("\"Concept\""));
    }

    #[test]
    fn the_graph_round_trips_through_json() {
        let graph = KnowledgeGraph {
            nodes: vec![ExtractedNode {
                name: "Tama".to_string(),
                node_type: ExtractedNodeType::Person,
                description: "A group member.".to_string(),
            }],
            edges: vec![ExtractedEdge {
                source: "Tama".to_string(),
                target: "GRPO".to_string(),
                relationship_name: "likes".to_string(),
                description: "Tama said she likes GRPO.".to_string(),
            }],
        };
        let json = serde_json::to_string(&graph).expect("to json");
        let back: KnowledgeGraph = serde_json::from_str(&json).expect("from json");
        assert_eq!(graph, back);
    }
}
