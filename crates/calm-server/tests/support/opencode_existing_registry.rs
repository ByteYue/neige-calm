use super::*;

#[tokio::test]
async fn registry_rejects_aliases_and_never_exposes_credentials() {
    let fixture = Fixture::new().await;
    let stack = Stack::boot(&fixture).await;
    let (status, body) = stack
        .request("GET", "/api/opencode/connections", None, None)
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["connections"][0]["id"], "original");
    assert!(!body.to_string().contains("password"));
    assert!(body["connections"][0].get("port").is_none());
    let mut config = calm_server::opencode_planner::attachment::ConnectionsConfig::read(
        &fixture.path().join("connections.json"),
    )
    .unwrap();
    let mut alias = config.connections[0].clone();
    alias.id = "alias".into();
    config.connections.push(alias);
    assert!(
        calm_server::opencode_planner::config::OpenCodePlannerHost::unconfigured_scratch()
            .unwrap()
            .with_connections(config)
            .is_err()
    );
    stack.shutdown().await;
}

#[tokio::test]
async fn registry_refuses_external_directory_owned_by_neige_workspace_recycling() {
    let fixture = Fixture::new().await;
    let config = calm_server::opencode_planner::attachment::ConnectionsConfig::read(
        &fixture.path().join("connections.json"),
    )
    .unwrap();
    let host = calm_server::opencode_planner::config::OpenCodePlannerHost::unconfigured_scratch()
        .unwrap()
        .with_connections(config)
        .unwrap();
    assert!(host.validate_external_directories(fixture.path()).is_err());
}

#[tokio::test]
async fn attachment_refuses_existing_external_kernel_worktree_and_its_child() {
    let fixture = Fixture::new().await;
    let stack = Stack::boot(&fixture).await;
    let track = stack.track(&fixture).await;
    let repo = fixture.path().join("user-repository");
    std::fs::create_dir_all(&repo).unwrap();
    let git = |args: &[&str]| {
        let output = std::process::Command::new("git")
            .arg("-C")
            .arg(&repo)
            .args(args)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
    };
    git(&["init", "-q"]);
    git(&[
        "-c",
        "user.name=Fixture",
        "-c",
        "user.email=fixture@example.invalid",
        "commit",
        "--allow-empty",
        "-qm",
        "initial",
    ]);
    let worktree = repo
        .join(".claude/worktrees")
        .join(format!("track-{track}"));
    git(&[
        "worktree",
        "add",
        "-q",
        "-b",
        "owned-track",
        worktree.to_str().unwrap(),
    ]);
    stack.shutdown().await;
    for directory in [worktree.clone(), worktree.join("etl-child")] {
        std::fs::create_dir_all(&directory).unwrap();
        fixture.native.0.lock().unwrap().directory = directory.clone();
        fixture.write_config(1);
        let reboot = Stack::boot(&fixture).await;
        sqlx::query("UPDATE tracks SET workspace_worktree_path=?1 WHERE id=?2")
            .bind(worktree.display().to_string())
            .bind(&track)
            .execute(&reboot.state.raw_repo().sqlite_pool().unwrap())
            .await
            .unwrap();

        let (status, body) = reboot
            .request(
                "POST",
                &format!("/api/tracks/{track}/opencode-conversations"),
                Some(json!({"connection_id":"original","session_id":SESSION})),
                Some("owned-worktree"),
            )
            .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
        assert!(fixture.posts().is_empty());
        assert!(reboot.journals().await.is_empty());
        sqlx::query("UPDATE tracks SET workspace_worktree_path=NULL WHERE id=?1")
            .bind(&track)
            .execute(&reboot.state.raw_repo().sqlite_pool().unwrap())
            .await
            .unwrap();
        reboot.shutdown().await;
    }
}

#[tokio::test]
async fn invalid_targets_version_and_cross_track_duplicate_leave_no_native_effects() {
    let fixture = Fixture::new().await;
    let stack = Stack::boot(&fixture).await;
    let track = stack.track(&fixture).await;
    for (key, target, status) in [
        (None, SESSION, StatusCode::BAD_REQUEST),
        (Some("missing"), "ses_missing", StatusCode::BAD_REQUEST),
    ] {
        let (actual, _) = stack
            .request(
                "POST",
                &format!("/api/tracks/{track}/opencode-conversations"),
                Some(json!({"connection_id":"original","session_id":target})),
                key,
            )
            .await;
        assert_eq!(actual, status);
    }
    fixture.native.0.lock().unwrap().version = "wrong";
    let (status, _) = stack
        .request(
            "POST",
            &format!("/api/tracks/{track}/opencode-conversations"),
            Some(json!({"connection_id":"original","session_id":SESSION})),
            Some("bad-version"),
        )
        .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    fixture.native.0.lock().unwrap().version = "1.18.34";
    let card = stack.attach(&track, "attach").await;
    let other = stack.track(&fixture).await;
    let (status, _) = stack
        .request(
            "POST",
            &format!("/api/tracks/{other}/opencode-conversations"),
            Some(json!({"connection_id":"original","session_id":SESSION})),
            Some("other-track"),
        )
        .await;
    assert_eq!(status, StatusCode::CONFLICT);
    let (status, _) = stack
        .request(
            "POST",
            &format!("/api/tracks/{track}/opencode-conversations"),
            Some(json!({"connection_id":"original","session_id":"ses_missing"})),
            Some("attach"),
        )
        .await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(stack.attach(&track, "attach").await, card);
    assert!(fixture.posts().is_empty());
    stack.shutdown().await;
}

#[tokio::test]
async fn attachment_ignores_unrelated_corrupt_cards_without_allowing_duplicate_targets() {
    let fixture = Fixture::new().await;
    let stack = Stack::boot(&fixture).await;
    let track = stack.track(&fixture).await;
    let other = stack.track(&fixture).await;
    let note = stack
        .state
        .raw_repo()
        .card_create(calm_server::model::NewCard {
            track_id: other.clone().into(),
            kind: "note".into(),
            sort: None,
            title: None,
            payload: json!({}),
        })
        .await
        .unwrap();
    sqlx::query("UPDATE cards SET payload='{not-json' WHERE id=?1")
        .bind(note.id.as_str())
        .execute(&stack.state.raw_repo().sqlite_pool().unwrap())
        .await
        .unwrap();
    let card = stack.attach(&track, "attach-with-corrupt-neighbor").await;
    stack.wait_text(&card, "original progress").await;
    assert_eq!(
        stack.attach(&track, "retry-with-corrupt-neighbor").await,
        card
    );
    let (status, _) = stack
        .request(
            "POST",
            &format!("/api/tracks/{other}/opencode-conversations"),
            Some(json!({"connection_id":"original","session_id":SESSION})),
            Some("duplicate-native-target"),
        )
        .await;
    assert_eq!(
        status,
        StatusCode::CONFLICT,
        "another track cannot attach the same native target"
    );
    assert!(fixture.posts().is_empty());
    stack.shutdown().await;
}
