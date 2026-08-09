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
    ///
    /// The `entities` alias tolerates field-name drift of endpoints
    /// that ignore the output schema and free-generate. Aliases affect
    /// deserialization only: serialization and the schemars schema keep
    /// the canonical name.
    #[serde(alias = "entities")]
    pub nodes: Vec<ExtractedNode>,
    /// The facts of the batch. Endpoints reference node names of this
    /// batch.
    ///
    /// The `relationships`/`facts` aliases tolerate endpoint
    /// field-name drift (deserialization only; see `nodes`).
    #[serde(alias = "relationships", alias = "facts")]
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
    ///
    /// The `type`/`kind` aliases tolerate endpoint field-name drift
    /// (deserialization only; serialization and the schema keep the
    /// canonical name). No alias on `name`/`description`: a missing
    /// description stays a hard deserialization error.
    #[serde(alias = "type", alias = "kind")]
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
    ///
    /// The lowercase alias tolerates endpoint drift of the enum
    /// spelling (deserialization only; the schema enum stays
    /// "Person"/"Concept").
    #[serde(alias = "person")]
    Person,
    /// An open-domain concept.
    ///
    /// The lowercase alias tolerates endpoint drift (see `Person`).
    #[serde(alias = "concept")]
    Concept,
}

/// One extracted fact.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, schemars::JsonSchema)]
pub struct ExtractedEdge {
    /// The `name` of the source node of this batch.
    ///
    /// The `from` alias tolerates endpoint field-name drift
    /// (deserialization only; see `KnowledgeGraph::nodes`).
    #[serde(alias = "from")]
    pub source: String,
    /// The `name` of the target node of this batch.
    ///
    /// The `to` alias tolerates endpoint field-name drift.
    #[serde(alias = "to")]
    pub target: String,
    /// snake_case relationship name (lowercase letters, digits,
    /// underscores; starts with a letter). The reserved system names
    /// contains, known_as, also_known_as, is_a, and supersedes must not
    /// be used (Section 6.3). Never trust the prompt: the name is
    /// validated again in Rust.
    ///
    /// The `relationship`/`relation`/`predicate` aliases tolerate
    /// endpoint field-name drift.
    #[serde(alias = "relationship", alias = "relation", alias = "predicate")]
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
        // The serde aliases are deserialization-only: the schema must
        // keep the canonical field names (schemars 1.x ignores serde
        // aliases).
        assert!(json.contains("\"nodes\""));
        assert!(json.contains("\"edges\""));
        assert!(json.contains("\"node_type\""));
        assert!(json.contains("\"relationship_name\""));
    }

    #[test]
    fn field_name_aliases_deserialize_into_the_canonical_struct() {
        // Endpoints that ignore the output schema free-generate; the
        // aliases tolerate the observed field-name drift
        // (deserialization only).
        let node: ExtractedNode =
            serde_json::from_str(r#"{"name":"Tama","type":"Person","description":"d"}"#)
                .expect("type alias");
        assert_eq!(node.node_type, ExtractedNodeType::Person);
        let node: ExtractedNode =
            serde_json::from_str(r#"{"name":"Tama","kind":"Person","description":"d"}"#)
                .expect("kind alias");
        assert_eq!(node.node_type, ExtractedNodeType::Person);
        // Lowercase enum drift: the variant aliases accept it.
        let node: ExtractedNode =
            serde_json::from_str(r#"{"name":"Tama","node_type":"person","description":"d"}"#)
                .expect("lowercase variant");
        assert_eq!(node.node_type, ExtractedNodeType::Person);
        let node: ExtractedNode =
            serde_json::from_str(r#"{"name":"GRPO","node_type":"concept","description":"d"}"#)
                .expect("lowercase variant");
        assert_eq!(node.node_type, ExtractedNodeType::Concept);

        for relationship_field in ["relationship", "relation", "predicate"] {
            let edge_json = format!(
                r#"{{"source":"Tama","target":"GRPO","{relationship_field}":"likes","description":"d"}}"#
            );
            let edge: ExtractedEdge = serde_json::from_str(&edge_json).expect(&edge_json);
            assert_eq!(edge.relationship_name, "likes");
        }
        let edge: ExtractedEdge = serde_json::from_str(
            r#"{"from":"Tama","to":"GRPO","relationship_name":"likes","description":"d"}"#,
        )
        .expect("from/to aliases");
        assert_eq!(edge.source, "Tama");
        assert_eq!(edge.target, "GRPO");

        // Graph-level container drift.
        let graph: KnowledgeGraph =
            serde_json::from_str(r#"{"entities":[],"facts":[]}"#).expect("entities/facts");
        assert!(graph.nodes.is_empty());
        assert!(graph.edges.is_empty());
        let graph: KnowledgeGraph =
            serde_json::from_str(r#"{"nodes":[],"relationships":[]}"#).expect("relationships");
        assert!(graph.edges.is_empty());
    }

    #[test]
    fn a_missing_description_stays_a_deserialization_error() {
        // No serde(default) on content fields: a missing description
        // is a hard error, never a silent empty string.
        let node = serde_json::from_str::<ExtractedNode>(r#"{"name":"Tama","node_type":"Person"}"#);
        assert!(node.is_err());
        let edge = serde_json::from_str::<ExtractedEdge>(
            r#"{"source":"Tama","target":"GRPO","relationship_name":"likes"}"#,
        );
        assert!(edge.is_err());
    }

    #[test]
    fn serialization_keeps_the_canonical_field_names() {
        // Aliases affect deserialization only: the serialized JSON and
        // the schemars schema keep the canonical names, so downstream
        // readers never see the drift spellings.
        let graph = KnowledgeGraph {
            nodes: vec![ExtractedNode {
                name: "Tama".to_string(),
                node_type: ExtractedNodeType::Person,
                description: "d".to_string(),
            }],
            edges: vec![ExtractedEdge {
                source: "Tama".to_string(),
                target: "GRPO".to_string(),
                relationship_name: "likes".to_string(),
                description: "d".to_string(),
            }],
        };
        let value = serde_json::to_value(&graph).expect("to value");
        assert!(value.get("nodes").is_some());
        assert!(value.get("edges").is_some());
        assert!(value.get("entities").is_none());
        assert!(value.get("relationships").is_none());
        assert!(value.get("facts").is_none());
        let node = &value["nodes"][0];
        assert!(node.get("node_type").is_some());
        assert!(node.get("type").is_none());
        assert!(node.get("kind").is_none());
        assert_eq!(node["node_type"], serde_json::json!("Person"));
        let edge = &value["edges"][0];
        assert!(edge.get("relationship_name").is_some());
        assert!(edge.get("relationship").is_none());
        assert!(edge.get("relation").is_none());
        assert!(edge.get("predicate").is_none());
        assert!(edge.get("source").is_some());
        assert!(edge.get("from").is_none());
        assert!(edge.get("target").is_some());
        assert!(edge.get("to").is_none());
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
