//! Live-API smoke test of the rig extractor. Ignored by default; run
//! with `TAMAKO_LIVE_TEST=1` and `ANTHROPIC_API_KEY` set:
//!
//! ```sh
//! TAMAKO_LIVE_TEST=1 cargo test -p tamako-agent --test live_extraction -- --ignored
//! ```

use tamako_agent::{
    BatchMessage, BindingSource, ExtractionInput, ExtractorConfig, KnowledgeExtractor,
    MentionBinding, RigExtractor,
};

#[tokio::test]
#[ignore = "live API test; run with TAMAKO_LIVE_TEST=1 and ANTHROPIC_API_KEY set"]
async fn the_rig_extractor_returns_a_parseable_graph() {
    if std::env::var("TAMAKO_LIVE_TEST").as_deref() != Ok("1") {
        return;
    }
    let extractor = RigExtractor::from_env(ExtractorConfig::default()).expect("provider config");
    let input = ExtractionInput {
        batch_id: "live-smoke".to_string(),
        messages: vec![
            BatchMessage {
                display_name: "Alice".to_string(),
                time_hhmm: "09:12".to_string(),
                text: "I finally deployed the migration to staging tonight".to_string(),
                forward: None,
            },
            BatchMessage {
                display_name: "Bob".to_string(),
                time_hhmm: "09:13".to_string(),
                text: "nice, did the rollback plan work?".to_string(),
                forward: None,
            },
        ],
        mention_map: vec![
            MentionBinding {
                display_name: "Alice".to_string(),
                tg_user_id: "1001".to_string(),
                source: BindingSource::Sender,
            },
            MentionBinding {
                display_name: "Bob".to_string(),
                tg_user_id: "2002".to_string(),
                source: BindingSource::Sender,
            },
        ],
        related_pairs: vec![],
        origins: vec![],
    };
    let graph = extractor.extract(&input, None).await.expect("extraction");
    // Live output is not deterministic; keep the assertions minimal.
    assert!(!graph.nodes.is_empty(), "expected at least one node");
}
