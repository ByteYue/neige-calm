//! A settled native tool step is progress while OpenCode still owes its final reply.
use super::{Root, Stack, wait};
use std::time::Duration;

pub(super) async fn assert_pending_final(root: &Root, stack: &Stack, card: &str) {
    wait(
        "completed command step projected before held final reply",
        || async {
            stack.transcript(card).await.iter().any(|row| {
                row["item"]["type"] == "commandExecution" && row["item"]["status"] == "completed"
            })
        },
    )
    .await;
    // Span multiple native snapshot polls with the completed local-tool step still latest.
    for _ in 0..4 {
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert!(
            stack.outcomes(card).await.is_empty(),
            "tool step falsely completed turn"
        );
        assert_eq!(stack.run(card).await["phase"], "turn_running");
        assert_eq!(
            root.requests().len(),
            1,
            "observations never resend the prompt"
        );
        assert!(
            !root.live_processes().is_empty(),
            "native loop must still own its final reply"
        );
    }
    std::fs::write(root.fake().join("release"), "final reply may now complete").unwrap();
    root.mode("complete");
}
