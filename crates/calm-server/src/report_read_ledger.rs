//! What each worker session last read of a report (#1877), so `calm.report.commit` can default its
//! `if_doc_rev` / `if_rev` / section anchors to it. Two entry points: a read that returned report
//! text records exactly the snapshot it rendered, and [`ReadLedger::record_authored`] records only
//! what the session's own commit wrote. The check itself stays in the persist tx.
//! Process memory like [`crate::plugin_results`]: a kernel restart forgets it and the next write is
//! refused until the session reads again. Eviction is lazy: the TTL on every access, and a session's
//! first read drops every other session's entries of the same card (at most one session of a card
//! is live, so those were superseded or exited).
//! Known gap, fails closed: a block the session creates inside a section it read is not added to
//! that section's recorded list, so the next write of the section needs a re-read.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use calm_types::track_report::ReportBlock;

use crate::plugin_results::Clock;
use crate::report_sections::sections;
use crate::track_report::Authored;

pub const TTL_MS: i64 = 2 * 60 * 60 * 1000;

/// One session's reads of one report, merged: the `docRev` of its latest read, and every block and
/// whole section any of its reads rendered, at the rev it rendered.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LastRead {
    /// Only a read that showed the docRev, summary and index (`calm.report.read`) sets it.
    pub doc_rev: Option<u64>,
    pub blocks: HashMap<String, u32>,
    /// Section title → its ordered `(id, rev)` list; only sections a read rendered whole.
    pub sections: HashMap<String, Vec<(String, u32)>>,
}

struct Entry {
    caller_card_id: String,
    read: LastRead,
    recorded_at: i64,
}

/// Keyed by `(worker session id, report card id)`.
pub struct ReadLedger {
    inner: Mutex<HashMap<(String, String), Entry>>,
    now: Clock,
}

impl Default for ReadLedger {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for ReadLedger {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReadLedger")
            .field("entries", &self.lock().len())
            .finish()
    }
}

impl ReadLedger {
    pub fn new() -> Self {
        Self::with_clock(Arc::new(crate::model::now_ms))
    }

    /// Test seam: an injectable clock for the TTL.
    pub fn with_clock(now: Clock) -> Self {
        Self {
            inner: Mutex::new(HashMap::new()),
            now,
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<(String, String), Entry>> {
        self.inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Record one text-returning read: `blocks` is the snapshot it rendered from, `rendered` the ids
    /// of the blocks its text carries, `doc_rev` the docRev it showed, if it showed one. A section
    /// counts as read only when all its blocks were rendered; a read of every block replaces the record.
    pub fn record(
        &self,
        session_id: &str,
        caller_card_id: &str,
        report_card_id: &str,
        doc_rev: Option<u64>,
        blocks: &[ReportBlock],
        rendered: &[String],
    ) {
        let now = (self.now)();
        let mut inner = self.lock();
        inner.retain(|(session, _), entry| {
            now - entry.recorded_at <= TTL_MS
                && (session == session_id || entry.caller_card_id != caller_card_id)
        });
        let entry = inner
            .entry((session_id.to_string(), report_card_id.to_string()))
            .or_insert_with(|| Entry {
                caller_card_id: caller_card_id.to_string(),
                read: LastRead::default(),
                recorded_at: now,
            });
        entry.recorded_at = now;
        let seen = |block: &ReportBlock| rendered.contains(&block.id);
        if blocks.iter().all(seen) {
            entry.read.blocks.clear();
            entry.read.sections.clear();
        }
        if doc_rev.is_some() {
            entry.read.doc_rev = doc_rev;
        }
        for block in blocks.iter().filter(|block| seen(block)) {
            entry.read.blocks.insert(block.id.clone(), block.rev);
        }
        let all = sections(blocks);
        for (title, range) in &all {
            let section = &blocks[range.clone()];
            // A duplicated title is refused at write time; recording either copy would be a guess.
            if all.iter().filter(|(other, _)| other == title).count() == 1
                && section.iter().all(seen)
            {
                entry.read.sections.insert(
                    (*title).to_string(),
                    section
                        .iter()
                        .map(|block| (block.id.clone(), block.rev))
                        .collect(),
                );
            }
        }
    }

    /// The second entry point: what the session's own commit wrote, in op order, at the revs it
    /// wrote; `doc_rev` is the committed docRev when the commit checked its document anchor. Blocks
    /// the commit did not write are never touched, so another writer's change stays a conflict.
    pub fn record_authored(
        &self,
        session_id: &str,
        report_card_id: &str,
        authored: &[Authored],
        doc_rev: Option<u64>,
    ) {
        let mut inner = self.lock();
        let Some(entry) = inner.get_mut(&(session_id.to_string(), report_card_id.to_string()))
        else {
            return;
        };
        let read = &mut entry.read;
        for write in authored {
            match write {
                Authored::Block(id, rev) => {
                    read.blocks.insert(id.clone(), *rev);
                    for (seen, seen_rev) in read.sections.values_mut().flatten() {
                        if seen == id {
                            *seen_rev = *rev;
                        }
                    }
                }
                Authored::Section(title, list) => {
                    read.blocks.extend(list.iter().cloned());
                    read.sections.insert(title.clone(), list.clone());
                }
                Authored::SectionDeleted(title) => {
                    read.sections.remove(title);
                }
            }
        }
        if doc_rev.is_some() {
            read.doc_rev = doc_rev;
        }
    }

    /// What `session_id` last read of the report, if anything (and not expired).
    pub fn last_read(&self, session_id: &str, report_card_id: &str) -> Option<LastRead> {
        let now = (self.now)();
        let mut inner = self.lock();
        inner.retain(|_, entry| now - entry.recorded_at <= TTL_MS);
        inner
            .get(&(session_id.to_string(), report_card_id.to_string()))
            .map(|entry| entry.read.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use calm_types::report_blocks::{reassign_ids, split_body};
    use std::sync::atomic::{AtomicI64, Ordering};

    fn ids(blocks: &[ReportBlock], indices: &[usize]) -> Vec<String> {
        indices.iter().map(|i| blocks[*i].id.clone()).collect()
    }

    #[test]
    fn records_rendered_blocks_and_only_whole_sections() {
        let ledger = ReadLedger::new();
        let blocks = reassign_ids(&[], &split_body("intro\n# A\na\n## A1\nx\n# B\nb\n"));
        ledger.record(
            "s",
            "planner",
            "report",
            Some(3),
            &blocks,
            &ids(&blocks, &[1, 3]),
        );
        let read = ledger.last_read("s", "report").expect("recorded");
        assert_eq!(read.doc_rev, Some(3));
        assert_eq!(read.blocks.len(), 2);
        assert_eq!(
            read.sections.keys().collect::<Vec<_>>(),
            vec!["B"],
            "A was rendered only in part"
        );
        assert_eq!(ledger.last_read("other", "report"), None);
    }

    #[test]
    fn expires_and_a_new_session_of_the_card_drops_its_predecessor() {
        let clock = Arc::new(AtomicI64::new(0));
        let now = clock.clone();
        let ledger = ReadLedger::with_clock(Arc::new(move || now.load(Ordering::SeqCst)));
        let blocks = reassign_ids(&[], &split_body("# A\na\n"));
        let all = ids(&blocks, &[0]);
        ledger.record("old", "planner", "report", Some(1), &blocks, &all);
        ledger.record(
            "assistant",
            "assistant-card",
            "report",
            Some(1),
            &blocks,
            &all,
        );
        ledger.record("new", "planner", "report", Some(1), &blocks, &all);
        assert_eq!(ledger.last_read("old", "report"), None, "superseded");
        assert!(ledger.last_read("assistant", "report").is_some());
        clock.store(TTL_MS + 1, Ordering::SeqCst);
        assert_eq!(ledger.last_read("new", "report"), None, "expired");
    }
}
