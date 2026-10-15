/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.planner.plan.nodes.exec.stream;

import org.apache.flink.table.test.program.SinkTestStep;
import org.apache.flink.table.test.program.SourceTestStep;
import org.apache.flink.table.test.program.TableTestProgram;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

/**
 * {@link TableTestProgram} definitions for testing {@link StreamExecLateralSnapshotJoin}.
 *
 * <p>The programs cover a savepoint taken in each of the operator's two phases; the
 * load_completed_time gate is at {@code 00:00:03} in both.
 *
 * <ul>
 *   <li>{@link #LATERAL_SNAPSHOT_JOIN_PHASE_LOAD}: the transition to JOIN is not triggered before
 *       the savepoint, so the operator is still in LOAD; the savepoint captures the partial build
 *       multi-set and the buffered probe row. The flip to JOIN phase happens only after restore.
 *   <li>{@link #LATERAL_SNAPSHOT_JOIN_PHASE_JOIN}: the whole build side is loaded before the
 *       savepoint (the last build-side row triggers the flip), so the operator has flipped to JOIN
 *       and its build-side snapshot is materialized and frozen when the stop-with-savepoint fires.
 * </ul>
 *
 * <p>Both verify that the build state and the LOAD/JOIN phase (union operator state) survive the
 * savepoint: after restore the "after restore" probe rows join the restored snapshot.
 */
public class LateralSnapshotJoinTestPrograms {

    static final String[] PROBE_SCHEMA = {
        "pk STRING",
        "pv INT",
        "pts_str STRING",
        "pts AS TO_TIMESTAMP(pts_str)",
        "WATERMARK FOR pts AS pts"
    };

    static final String[] BUILD_SCHEMA = {
        "bk STRING",
        "bv INT",
        "bts_str STRING",
        "bts AS TO_TIMESTAMP(bts_str)",
        "WATERMARK FOR bts AS bts"
    };

    static final String[] SINK_SCHEMA = {"pk STRING", "pv INT", "bk STRING", "bv INT"};

    // Two rows for key 'a' exercise the per-key multi-set; the last row's watermark (00:00:03)
    // reaches the gate and flips the operator to JOIN.
    static final Row[] BUILD_BEFORE_DATA = {
        Row.of("a", 10, "2020-01-01 00:00:01"),
        Row.of("b", 20, "2020-01-01 00:00:02"),
        Row.of("a", 11, "2020-01-01 00:00:03")
    };

    static final Row[] PROBE_BEFORE_DATA = {
        Row.of("a", 100, "2020-01-01 00:00:06"), Row.of("b", 200, "2020-01-01 00:00:07")
    };

    // 'a' matches the restored snapshot; 'c' has no match.
    static final Row[] PROBE_AFTER_DATA = {
        Row.of("a", 101, "2020-01-01 00:00:10"), Row.of("c", 300, "2020-01-01 00:00:11")
    };

    // LOAD-phase restore scenario: none of these build rows reaches the 00:00:03 gate, so the
    // operator is still in LOAD when the savepoint is taken. The number of rows is the count the
    // savepoint trigger waits for (see LateralSnapshotJoinRestoreTest#awaitSavepointReady).
    static final Row[] LOAD_BUILD_BEFORE_DATA = {
        Row.of("a", 10, "2020-01-01 00:00:01"), Row.of("a", 11, "2020-01-01 00:00:02")
    };

    // The gate row (00:00:03) arrives only after restore and flips the operator to JOIN, draining
    // the probe rows buffered before the savepoint.
    static final Row[] LOAD_BUILD_AFTER_DATA = {Row.of("a", 12, "2020-01-01 00:00:03")};

    // Buffered during LOAD (LOAD emits nothing), so it survives the savepoint in probe state.
    static final Row[] LOAD_PROBE_BEFORE_DATA = {Row.of("a", 100, "2020-01-01 00:00:06")};

    static final Row[] LOAD_PROBE_AFTER_DATA = {Row.of("a", 101, "2020-01-01 00:00:10")};

    private static final String SNAPSHOT_BUILD =
            "LATERAL SNAPSHOT("
                    + "input => TABLE b, on_time => DESCRIPTOR(bts), "
                    + "load_completed_time => CAST(TIMESTAMP '2020-01-01 00:00:03' AS TIMESTAMP_LTZ(3))"
                    + ") AS s ON probe.pk = s.bk";

    // Build side with a primary key fed by an upsert changelog (I, UA, D). Its upsert key (bk)
    // contains the join key, so the join uses the upsert-keyed build-side layout.
    static final String[] BUILD_UPSERT_SCHEMA = {
        "bk STRING",
        "bv INT",
        "bts_str STRING",
        "bts AS TO_TIMESTAMP(bts_str)",
        "WATERMARK FOR bts AS bts",
        "PRIMARY KEY (bk) NOT ENFORCED"
    };

    // Before the savepoint, all applied by upsert key during LOAD and materialized at the flip
    // (every row's row-time is <= the 00:00:03 gate): insert a and b, delete b, then update a to 12.
    // The last row's watermark (00:00:03) reaches the gate and flips to JOIN. Committed snapshot:
    // {a=12} (b was deleted by key). Keeping all build changes before the gate avoids a race with
    // the after-restore probes, so the restored upsert state (with an applied update and delete) is
    // what the after-restore probes observe.
    static final Row[] UPSERT_BUILD_BEFORE_DATA = {
        Row.ofKind(RowKind.INSERT, "a", 10, "2020-01-01 00:00:01"),
        Row.ofKind(RowKind.INSERT, "b", 20, "2020-01-01 00:00:02"),
        Row.ofKind(RowKind.DELETE, "b", 20, "2020-01-01 00:00:02"),
        Row.ofKind(RowKind.UPDATE_AFTER, "a", 12, "2020-01-01 00:00:03")
    };

    // After restore: a matches the restored row (12); b was deleted, so it has no match.
    static final Row[] UPSERT_PROBE_AFTER_DATA = {
        Row.of("a", 101, "2020-01-01 00:00:10"), Row.of("b", 201, "2020-01-01 00:00:11")
    };

    private static final String SNAPSHOT_UPSERT_BUILD =
            "LATERAL SNAPSHOT("
                    + "input => TABLE b_upsert, on_time => DESCRIPTOR(bts), "
                    + "load_completed_time => CAST(TIMESTAMP '2020-01-01 00:00:03' AS TIMESTAMP_LTZ(3))"
                    + ") AS s ON probe.pk = s.bk";

    // Restore taken while the operator is in LOAD phase: the savepoint captures the partial build
    // multi-set and the buffered probe row, the LOAD phase is recorded in union operator state, and
    // nothing has been emitted yet. After restore the 00:00:03 build row completes the load, flips
    // to JOIN, and the buffered + after-restore probe rows join the full snapshot {10, 11, 12} for
    // key 'a'.
    public static final TableTestProgram LATERAL_SNAPSHOT_JOIN_PHASE_LOAD =
            TableTestProgram.of(
                            "lateral-snapshot-join-load-phase",
                            "validates a LATERAL SNAPSHOT inner join restored from a LOAD-phase savepoint")
                    .setupTableSource(
                            SourceTestStep.newBuilder("probe")
                                    .addSchema(PROBE_SCHEMA)
                                    .producedBeforeRestore(LOAD_PROBE_BEFORE_DATA)
                                    .producedAfterRestore(LOAD_PROBE_AFTER_DATA)
                                    .build())
                    .setupTableSource(
                            SourceTestStep.newBuilder("b")
                                    .addSchema(BUILD_SCHEMA)
                                    .producedBeforeRestore(LOAD_BUILD_BEFORE_DATA)
                                    .producedAfterRestore(LOAD_BUILD_AFTER_DATA)
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink")
                                    .addSchema(SINK_SCHEMA)
                                    // Empty before restore: LOAD phase emits nothing before the
                                    // savepoint. The explicit String[] picks the String overload
                                    // and marks this a SINK_WITH_RESTORE_DATA step.
                                    .consumedBeforeRestore(new String[0])
                                    .consumedAfterRestore(
                                            "+I[a, 100, a, 10]",
                                            "+I[a, 100, a, 11]",
                                            "+I[a, 100, a, 12]",
                                            "+I[a, 101, a, 10]",
                                            "+I[a, 101, a, 11]",
                                            "+I[a, 101, a, 12]")
                                    .build())
                    .runSql(
                            "INSERT INTO sink SELECT probe.pk, probe.pv, s.bk, s.bv "
                                    + "FROM probe JOIN "
                                    + SNAPSHOT_BUILD)
                    .build();

    // Restore taken while the operator is in JOIN phase
    public static final TableTestProgram LATERAL_SNAPSHOT_JOIN_PHASE_JOIN =
            TableTestProgram.of(
                            "lateral-snapshot-join-inner",
                            "validates a LATERAL SNAPSHOT inner join across a restore")
                    .setupTableSource(
                            SourceTestStep.newBuilder("probe")
                                    .addSchema(PROBE_SCHEMA)
                                    .producedBeforeRestore(PROBE_BEFORE_DATA)
                                    .producedAfterRestore(PROBE_AFTER_DATA)
                                    .build())
                    .setupTableSource(
                            SourceTestStep.newBuilder("b")
                                    .addSchema(BUILD_SCHEMA)
                                    .producedBeforeRestore(BUILD_BEFORE_DATA)
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink")
                                    .addSchema(SINK_SCHEMA)
                                    .consumedBeforeRestore(
                                            "+I[a, 100, a, 10]",
                                            "+I[a, 100, a, 11]",
                                            "+I[b, 200, b, 20]")
                                    .consumedAfterRestore("+I[a, 101, a, 10]", "+I[a, 101, a, 11]")
                                    .build())
                    .runSql(
                            "INSERT INTO sink SELECT probe.pk, probe.pv, s.bk, s.bv "
                                    + "FROM probe JOIN "
                                    + SNAPSHOT_BUILD)
                    .build();

    // Restore of the upsert-keyed build-side layout: the build side is an upsert source with a
    // primary key, so build updates/deletes apply by key and no ChangelogNormalize is inserted.
    // Across the savepoint the upsert-keyed build state survives; after restore an update (a -> 12)
    // and a delete (b) are applied by key, which the after-restore probes observe.
    public static final TableTestProgram LATERAL_SNAPSHOT_JOIN_UPSERT_BUILD =
            TableTestProgram.of(
                            "lateral-snapshot-join-upsert-build",
                            "validates a LATERAL SNAPSHOT inner join with an upsert (partial-update) build side across a restore")
                    .setupTableSource(
                            SourceTestStep.newBuilder("probe")
                                    .addSchema(PROBE_SCHEMA)
                                    .producedBeforeRestore(PROBE_BEFORE_DATA)
                                    .producedAfterRestore(UPSERT_PROBE_AFTER_DATA)
                                    .build())
                    .setupTableSource(
                            SourceTestStep.newBuilder("b_upsert")
                                    .addSchema(BUILD_UPSERT_SCHEMA)
                                    .addOption("changelog-mode", "I,UA,D")
                                    .producedBeforeRestore(UPSERT_BUILD_BEFORE_DATA)
                                    .build())
                    .setupTableSink(
                            SinkTestStep.newBuilder("sink")
                                    .addSchema(SINK_SCHEMA)
                                    .consumedBeforeRestore("+I[a, 100, a, 12]")
                                    .consumedAfterRestore("+I[a, 101, a, 12]")
                                    .build())
                    .runSql(
                            "INSERT INTO sink SELECT probe.pk, probe.pv, s.bk, s.bv "
                                    + "FROM probe JOIN "
                                    + SNAPSHOT_UPSERT_BUILD)
                    .build();
}
