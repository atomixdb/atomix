use crate::wal::*;

use std::collections::VecDeque;

use async_trait::async_trait;
use flatbuf::rangeserver_flatbuffers::range_server::*;
use flatbuffers::FlatBufferBuilder;
use prost::Message;
use proto::rangeserver::ReplicatedCommitRequest;
use tokio::sync::Mutex;

pub struct InMemoryWal {
    state: Mutex<State>,
}

struct State {
    first_offset: Option<u64>,
    entries: VecDeque<Vec<u8>>,
    flatbuf_builder: FlatBufferBuilder<'static>,
}

impl State {
    fn append_data_currently_in_builder(&mut self) -> Result<(), Error> {
        let bytes = self.flatbuf_builder.finished_data();
        let buf = Vec::from(bytes);
        self.entries.push_back(buf);
        self.flatbuf_builder.reset();
        Ok(())
    }
}

pub struct InMemIterator<'a> {
    index: u64,
    wal: &'a InMemoryWal,
    current_entry: Option<Vec<u8>>,
}

impl<'a> Iterator<'a> for InMemIterator<'a> {
    async fn next(&mut self) -> Option<LogEntry<'_>> {
        let wal = self.wal.state.lock().await;
        let ind = (wal.first_offset.unwrap_or(0) + self.index) as usize;
        if ind >= wal.entries.len() {
            return None;
        }
        self.current_entry = Some(wal.entries.get(ind).unwrap().clone());

        self.index += 1;
        self.current_entry
            .as_ref()
            .map(|e| root_as_log_entry(e).unwrap())
    }

    async fn next_offset(&self) -> Result<u64, Error> {
        let wal = self.wal.state.lock().await;
        match wal.first_offset {
            None => Ok(0),
            Some(first_offset) => {
                let offset = first_offset + self.index;
                Ok(offset)
            }
        }
    }
}

impl Default for InMemoryWal {
    fn default() -> Self {
        Self::new()
    }
}

impl InMemoryWal {
    pub fn new() -> Self {
        InMemoryWal {
            state: Mutex::new(State {
                first_offset: None,
                entries: VecDeque::new(),
                flatbuf_builder: FlatBufferBuilder::new(),
            }),
        }
    }
}

#[async_trait]
impl Wal for InMemoryWal {
    async fn sync(&self) -> Result<(), Error> {
        Ok(())
    }

    async fn first_offset(&self) -> Result<Option<u64>, Error> {
        let wal = self.state.lock().await;
        Ok(wal.first_offset)
    }

    async fn next_offset(&self) -> Result<u64, Error> {
        let wal = self.state.lock().await;
        let len_u64 = wal.entries.len() as u64;
        Ok(wal.first_offset.unwrap_or(0) + len_u64)
    }

    async fn append_prepare(&self, entry: PrepareRequest<'_>) -> Result<(), Error> {
        let mut state = self.state.lock().await;
        let prepare_bytes = state.flatbuf_builder.create_vector(entry._tab.buf());
        let fb_root = LogEntry::create(
            &mut state.flatbuf_builder,
            &LogEntryArgs {
                entry: Entry::Prepare,
                bytes: Some(prepare_bytes),
            },
        );
        state.flatbuf_builder.finish(fb_root, None);
        state.append_data_currently_in_builder()
    }

    async fn append_commit(&self, entry: CommitRequest<'_>) -> Result<(), Error> {
        let mut state = self.state.lock().await;
        let commit_bytes = state.flatbuf_builder.create_vector(entry._tab.buf());
        let fb_root = LogEntry::create(
            &mut state.flatbuf_builder,
            &LogEntryArgs {
                entry: Entry::Commit,
                bytes: Some(commit_bytes),
            },
        );
        state.flatbuf_builder.finish(fb_root, None);
        state.append_data_currently_in_builder()
    }

    async fn append_abort(&self, entry: AbortRequest<'_>) -> Result<(), Error> {
        let mut state = self.state.lock().await;
        let abort_bytes = state.flatbuf_builder.create_vector(entry._tab.buf());
        let fb_root = LogEntry::create(
            &mut state.flatbuf_builder,
            &LogEntryArgs {
                entry: Entry::Commit,
                bytes: Some(abort_bytes),
            },
        );
        state.flatbuf_builder.finish(fb_root, None);
        state.append_data_currently_in_builder()
    }

    async fn append_replicated_commit(&self, entry: ReplicatedCommitRequest) -> Result<(), Error> {
        let mut state = self.state.lock().await;
        let bytes = entry.encode_to_vec();
        let commit_bytes = state.flatbuf_builder.create_vector(&bytes);
        let fb_root = LogEntry::create(
            &mut state.flatbuf_builder,
            &LogEntryArgs {
                entry: Entry::ReplicatedCommit,
                bytes: Some(commit_bytes),
            },
        );
        state.flatbuf_builder.finish(fb_root, None);
        state.append_data_currently_in_builder()
    }

    async fn trim_before_offset(&self, offset: u64) -> Result<(), Error> {
        let mut wal = self.state.lock().await;
        while !wal.entries.is_empty() && wal.first_offset.unwrap_or(0) < offset {
            wal.entries.pop_front();
            wal.first_offset = Some(wal.first_offset.unwrap_or(0) + 1);
        }
        Ok(())
    }

    fn iterator(&self) -> InMemIterator {
        InMemIterator {
            index: 0,
            wal: self,
            current_entry: None,
        }
    }
}
