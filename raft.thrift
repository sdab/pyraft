
/**
 * Struct definitions for rpc API
 */
struct AppendReturn {
1: required i32 term,
2: required bool success,
}

struct VoteReturn {
1: required i32 term,
2: required bool voteGranted,
}

/**
 * Defines the rpc API for Raft
 */
service Raft {
  // Not part of the Raft API, but useful for debugging. Returns the string "pong"
  string ping();

  AppendReturn appendEntries(
    1: required i32 term,
    2: required i32 leaderId,
    3: required i32 prevLogIndex,
    4: required i32 prevLogTerm,
    5: required list<string> entries,
    6: required i32 leaderCommit,
    );

  VoteReturn requestVote(
    1: required i32 term,
    2: required i32 candidateId,
    3: required i32 lastLogIndex,
    4: required i32 lastLogTerm,
    );

  // optional call, here for completeness
  i32 installSnapshot(
    1: required i32 term,
    2: required i32 leaderId,
    3: required i32 lastIncludedIndex,
    4: required i32 lastIncludedTerm,
    5: required i32 offset,
    6: required binary data,
    7: required bool done,
    );
}
