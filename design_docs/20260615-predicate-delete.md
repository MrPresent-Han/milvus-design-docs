# MEP: Predicate Delete

- **Created:** 2026-06-15
- **Author(s):** @MrPresent-Han
- **Status:** Draft
- **Component:** Proxy, StreamingNode, QueryNode, DataCoord, DataNode, Storage
- **Related Issues:** TBD
- **Released:** TBD

## Summary

Introduce **Predicate Delete** as a parallel delete-by-expression lifecycle for Milvus. Instead of expanding a complex `delete by expr` request into a potentially huge list of `(pk, ts)` delete rows, Milvus persists a small number of durable predicate events, applies them online for query correctness, forwards them through L0 compaction, and finally drops matched rows during physical rewrite.

The end-to-end flow is:

```text
delete expr
  -> Proxy: PredicateDelete WAL event
  -> StreamingNode: predicate-delta L0 persistence
  -> QueryNode: replay and evaluate for online correctness
  -> DataNode: L0 compaction forwards pending events
  -> DataNode / milvus-storage: rewrite drops matched rows
```

The existing primary-key delete path remains unchanged. Predicate Delete is a separate event-log path with independent storage, metadata, replay, forwarding, and rewrite semantics.

## Motivation

Milvus currently handles complex `delete by expr` by first querying all matching primary keys and then writing ordinary `(pk, ts)` delete rows. This works well when the expression matches a small number of rows, but it scales poorly when the expression matches a large portion of a collection.

The current path has several amplification problems:

- **WAL IO amplification.** A single expression delete expands into many primary-key delete payloads. WAL write, replication, and recovery costs grow linearly with the number of matched rows.
- **StreamingNode buffer pressure.** StreamingNode must cache, count, sync, and flush large primary-key/timestamp batches as ordinary delete rows.
- **L0 compaction pressure.** Massive primary-key delete batches produce many L0 carriers and increase both scheduling and execution pressure for L0 compaction.
- **Delegator replay pressure.** QueryNode delegators must load and replay large L0 delete segments, slowing recovery/load and increasing CPU usage on the delegator node.

Predicate Delete changes the scaling unit from "matched rows" to "delete predicates". A large expression delete becomes one predicate event per affected vchannel, while QueryNode and compaction maintain correctness and eventual physical convergence.

## Public Interfaces

### Configuration

Add a feature gate to control whether complex delete expressions use Predicate Delete or the legacy query-PK fallback:

| Key | Default | Refresh | Description |
|-----|---------|---------|-------------|
| `common.enablePredicateDelete` | `false` | dynamic | When enabled, complex delete expressions are written as PredicateDelete events. Simple primary-key equality and `IN` deletes still use ordinary PK delete. |

### WAL payload

Predicate Delete extends the existing delete request payload instead of introducing a new streaming message type. The authoritative payload is the serialized expression plan, not the original expression string.

```text
PredicateDeleteRequest:
  expr                  // diagnostic only
  serialized_expr_plan  // proto.Marshal(planpb.PlanNode), correctness source
  delete_timestamp      // MVCC timestamp
  schema_version        // delete-time schema version
  partition_ids         // authoritative delete scope
```

Important semantics:

- `serialized_expr_plan` is the correctness source. Consumers must not re-parse `expr` for execution.
- `partition_ids` is the authoritative scope. Primary-key fields and `NumRows` are not authoritative for Predicate Delete.
- `schema_version` binds the event to the delete-time schema so replay and compaction evaluate the expression under the correct schema.
- Predicate event count is not deleted row count and must not feed delete ratio, `TotalRows`, or deleted-row metrics.

### Predicate-delta file schema

StreamingNode persists Predicate Delete events as predicate-delta logs. One Arrow row represents one predicate event.

```text
metadata:
  milvus.predicate_delta.format_version = 1
columns:
  source_msg_id
  delete_ts
  schema_version
  partition_ids
  expr
  serialized_expr_plan
```

### Segment metadata

`SegmentInfo` gains a PredicateDeltalogs summary that DataCoord can read without opening predicate-delta files.

The summary should include at least:

- predicate event count
- log size
- timestamp range
- segment-level union of all event-level `partition_ids`

For Storage V3, the real file paths are recorded in an independent `predicate_delta_logs` section in the manifest.

## Design Details

### Design principles

1. **Parallel path.** Primary-key delete remains concrete `(pk, ts)` rows. Predicate Delete is an expression event and does not reuse PK deltalog semantics.
2. **Independent accounting.** Predicate event count is not deleted row count.
3. **Serialized plan execution.** `serialized_expr_plan` is the correctness source; `expr` is for diagnostics only.
4. **Schema binding.** Every event carries `schema_version`; evaluators use the delete-time schema.
5. **Online correctness before physical convergence.** QueryNode guarantees online visibility; L0 compaction forwards pending events; rewrite eventually removes matched rows physically.

### Proxy / WAL producer

Proxy reuses the existing `getPrimaryKeysFromPlan` decision point:

- `pk == x` and `pk in [...]` still use ordinary simple delete.
- Other expressions use Predicate Delete when `common.enablePredicateDelete` is enabled.
- When the feature gate is disabled, the legacy query-PK fallback remains available.

Predicate Delete has no primary-key hash, so Proxy cannot route it to a single vchannel by key. It must append the same predicate event to every vchannel of the target collection.

```go
pk, simple := getPrimaryKeysFromPlan(plan)
if simple {
    return simpleDelete(pk)
}
if enablePredicateDelete {
    return AppendAllVChannels(PredicateDelete{
        Expr:               expr,
        SerializedExprPlan: serializedPlan,
        DeleteTs:           deleteTs,
        SchemaVersion:      schemaVersion,
        PartitionIDs:       partitionIDs,
    })
}
return complexDeleteByQueryPK(plan)
```

### StreamingNode / flushcommon predicate-delta persistence

StreamingNode does not evaluate Predicate Delete expressions. It only provides durable persistence.

Predicate Delete must use independent buffer, writer, and metadata paths:

- It must not be appended to `storage.DeleteData`.
- It must not be counted as deleted rows.
- It must be written as predicate-delta event logs, where one row is one predicate event.

A multi-partition predicate event must not be fanned out into multiple L0 segments. The L0 carrier can reuse existing abstractions, but Predicate Delete is organized as a channel/WAL-level carrier rather than a `partitionID + channel` ordinary delete carrier. The full partition scope remains in each event's `partition_ids`.

When StreamingNode flushes a Predicate L0 segment, it computes the union of all event-level `partition_ids` in that segment and stores the union in the Predicate L0 segment metadata. This allows DataCoord to schedule without opening predicate-delta files.

```text
DeleteMessageV1(predicate_delete)
  -> PredicateDeltaBuffer.Append(event)
  -> PredicateDeltalogWriter writes Arrow file
  -> SegmentInfo.PredicateDeltalogs summary
  -> V3 manifest.predicate_delta_logs paths
```

### QueryNode / segcore online correctness

QueryNode must identify Predicate Delete before ordinary primary-key delete alignment and validation. Predicate Delete has no `PrimaryKeys` or `Timestamps`; treating it as PK delete would incorrectly reject or mis-handle the event.

The shard delegator maintains an independent predicate buffer for online forwarding and sealed-segment load catch-up. Raw predicate events must not be stored in `storage.DeleteData` or ordinary PK delete buffer items.

Online behavior:

- For growing and sealed segments that are already online, QueryNode applies incoming predicate events immediately.
- Because there is no primary key, QueryNode cannot use bloom-filter pruning. It selects candidate segments by partition scope and asks segcore to evaluate the expression.
- Segcore evaluates the serialized plan against segment rows and inserts matched row offsets into the existing MVCC delete model as `DeletedRecord(delete_ts, row_offset)`.

The sealed-segment loading path is the no-gap correctness boundary. A segment can join distribution only after all relevant persisted and buffered predicate events are applied.

```text
ProcessPredicateDelete(event):
  buffer event
  forward to online target segments

Load sealed segment:
  load base data and persisted predicate deltalogs
  snapshot predicateBuffer after segment start ts
  apply snapshot and catch up new events
  add distribution under deleteMut.RLock
```

Segcore apply semantics:

```text
ApplyPredicateDelete(serializedPlan, deleteTs):
  parse planpb.PlanNode
  evaluate predicate on segment rows
  insert matched offsets into DeletedRecord(deleteTs, row_offset)
```

### DataCoord / DataNode L0 compaction forwarding

L0 compaction does not evaluate predicate expressions. It only forwards pending predicate events from L0 carriers to potentially affected L1/L2 target segments.

DataCoord scheduling rules:

- Read only `SegmentInfo.PredicateDeltalogs` summary.
- Do not open predicate-delta files during scheduling.
- Use the predicate summary's partition union to compute target partition scope.
- Fast-finish when there is no eligible target segment.

Predicate L0 segments should be organized separately from ordinary PK-delete L0 segments. A single L0 segment should not contain both ordinary `Deltalogs` and `PredicateDeltalogs`. If a later L0 compaction task selects both predicate L0 inputs and PK-delete L0 inputs, it must still preserve ordinary delete partition-scope grouping constraints and must not accidentally widen the scope of PK deletes.

DataNode opens input L0 manifests and enumerates both ordinary delta logs and predicate-delta logs:

- Ordinary delta logs follow the existing bloom-filter / primary-key split path.
- Predicate-delta logs are forwarded by each event's `partition_ids`.
- Compaction result metadata can contain both `Deltalogs` and `PredicateDeltalogs`, but predicate events must not be written into ordinary `Deltalogs`.

```go
type DeletePartitionScope struct {
    All        bool
    Partitions []int64
}

for event in readPredicateEvents(inputL0s) {
    for target in targetsInScope(event.PartitionIDs) {
        writePredicateEventToTarget(target, event)
    }
}
```

### milvus-storage delete-aware reader / physical rewrite

Physical rewrite is where Predicate Delete converges into actual row removal. The storage reader computes a row-level alive mask from manifest delete metadata, and the Milvus compaction writer appends only alive rows into the new segment.

Phase 1 keeps row copying in Milvus instead of moving filtering/projecting/copying into storage. `milvus-storage` returns aligned `RecordBatch + keep_mask`, where bit `1` means the row is alive.

The reader must be created from the full manifest, not only column groups, because column groups do not contain delete metadata.

```text
Reader::create(manifest, schema, needed_columns, properties)
  -> DeleteEvaluator::Create(manifest)
  -> base_reader.ReadNext(batch)
  -> keep_mask = evaluator.EvaluateKeepMask(batch)
  -> return {batch, keep_mask}
```

`DeleteEvaluator` loads both ordinary PK delta and predicate-delta metadata:

- PK delete builds `pk -> max_delete_ts`.
- Predicate Delete converts `serialized_expr_plan` to the storage expression IR and evaluates it with Arrow compute.
- All delete types respect MVCC: a row is deleted only when `row_ts <= delete_ts`.
- Predicate result `null` does not delete the row. Only definite `true` deletes.

```text
deleted = (pk_match(row) AND row_ts <= pk_delete_ts)
       OR (predicate_true(row) AND row_ts <= predicate_delete_ts)
keep_mask = NOT deleted
```

Phase 1 supports only simple scalar predicates:

- comparison
- `AND` / `OR` / `NOT`
- `IS NULL`
- `IS NOT NULL`
- optional `AlwaysTrue`

Unsupported nodes such as JSON, array, text match, function calls, and complex casts must fail fast.

## Compatibility, Deprecation, and Migration Plan

Predicate Delete is gated by `common.enablePredicateDelete` and defaults to off. When disabled, complex delete expressions continue to use the existing query-PK fallback.

Rolling upgrade must be protected by feature/version gates. Older QueryNode or DataNode versions must not silently ignore Predicate Delete events. A cluster should enable the feature only after all components that can consume delete WAL, predicate-delta logs, L0 compaction inputs, and rewrite metadata understand the new format.

No existing PK delete behavior is deprecated by this MEP. Simple primary-key equality and `IN` deletes keep their current concrete `(pk, ts)` representation.

## Test Plan

### Functional tests

- Verify simple primary-key equality and `IN` deletes still use ordinary PK delete.
- Verify complex expressions produce one Predicate Delete event per collection vchannel when the feature gate is enabled.
- Verify complex expressions fall back to query-PK delete when the feature gate is disabled.
- Verify partition-scoped Predicate Delete affects only target partitions.
- Verify `expr` is diagnostic only by executing from `serialized_expr_plan`.
- Verify delete-time `schema_version` is used during replay and compaction.

### Query correctness tests

- Apply Predicate Delete to growing segments and confirm search/query results exclude matched rows at delete timestamp.
- Apply Predicate Delete to sealed online segments and confirm matched rows are hidden.
- Load a sealed segment while predicate events arrive and verify the no-gap replay/buffer/catch-up sequence.
- Verify MVCC semantics: rows with `row_ts > delete_ts` remain visible.
- Verify predicate result `null` does not delete the row.

### Persistence and compaction tests

- Verify StreamingNode writes predicate-delta logs with the expected schema and metadata.
- Verify `SegmentInfo.PredicateDeltalogs` contains event count, log size, timestamp range, and partition union.
- Verify DataCoord schedules L0 forwarding from metadata without opening predicate-delta files.
- Verify DataNode forwards predicate events by event-level `partition_ids`.
- Verify compaction outputs ordinary `Deltalogs` and `PredicateDeltalogs` separately.
- Verify physical rewrite drops matched rows and preserves unmatched rows.

### Upgrade and guardrail tests

- Verify old-version consumers reject or block Predicate Delete instead of ignoring it.
- Verify Predicate Delete event count does not affect deleted-row metrics, delete ratio, or `TotalRows`.
- Verify unsupported storage expression nodes fail fast during physical rewrite.

## Rejected Alternatives

### Keep query-PK expansion for all complex expressions

This keeps implementation simple but preserves the current amplification problem. WAL IO, StreamingNode buffers, L0 compaction, and delegator replay still scale with matched row count instead of predicate count.

### Reuse ordinary PK deltalog format

Predicate Delete is not a list of primary keys and does not have ordinary delete row semantics. Reusing `storage.DeleteData` would make event count look like row count, confuse metrics and compaction policies, and make partition scope ambiguous for multi-partition predicates.

### Evaluate predicates during L0 compaction forwarding

L0 forwarding should remain a routing step. Evaluating expressions there would require loading target rows and delete-time schemas in the L0 compactor, increasing cost and coupling. The design instead forwards events to candidate targets and leaves row evaluation to QueryNode online apply or storage rewrite.

### Fan out multi-partition predicate events into per-partition L0 segments

Fanning out makes scheduling look similar to ordinary delete, but it duplicates events, loses the original event scope, and increases metadata/write amplification. The selected design keeps event-level `partition_ids` and stores a segment-level union only as scheduling metadata.

## References

- Source design report: `~/hc-claude-projects/milvus/equality-delete/predicate-delete-overall-report.md`
