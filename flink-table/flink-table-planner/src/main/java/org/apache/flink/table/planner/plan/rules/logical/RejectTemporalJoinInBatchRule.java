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

package org.apache.flink.table.planner.plan.rules.logical;

import org.apache.flink.table.api.TableException;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalCorrelate;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalSnapshot;

/**
 * Rules that reject temporal table joins ({@code FOR SYSTEM_TIME AS OF}) in batch mode with a clear
 * error message.
 *
 * <p>In the batch {@code EXPAND_PLAN_RULES}, lookup join rules run first and rewrite valid lookup
 * joins (processing-time + {@code LookupTableSource}) into {@code TemporalJoin} nodes. Any
 * remaining {@link LogicalCorrelate} + {@link LogicalSnapshot} pattern therefore represents an
 * unsupported temporal join. These rules catch it and throw a {@link TableException} rather than
 * letting the correlate survive into {@code FlinkDecorrelateProgram}, where it would cause a
 * confusing "unexpected correlate variable" internal error.
 */
public class RejectTemporalJoinInBatchRule extends RelOptRule {

    private static final String MESSAGE =
            "Temporal joins (FOR SYSTEM_TIME AS OF) on regular tables are not supported in "
                    + "batch mode. Use a lookup join or switch to streaming mode.";

    /**
     * Matches temporal joins where the right side of the Correlate is a Filter wrapping a Snapshot
     * (non-trivial join condition).
     */
    public static final RelOptRule WITH_FILTER =
            new RejectTemporalJoinInBatchRule(
                    operand(
                            LogicalCorrelate.class,
                            operand(RelNode.class, any()),
                            operand(
                                    LogicalFilter.class,
                                    operand(LogicalSnapshot.class, operand(RelNode.class, any())))),
                    "RejectTemporalJoinInBatchRuleWithFilter");

    /**
     * Matches temporal joins where the right side of the Correlate is a Snapshot directly (trivial
     * join condition).
     */
    public static final RelOptRule WITHOUT_FILTER =
            new RejectTemporalJoinInBatchRule(
                    operand(
                            LogicalCorrelate.class,
                            operand(RelNode.class, any()),
                            operand(LogicalSnapshot.class, operand(RelNode.class, any()))),
                    "RejectTemporalJoinInBatchRuleWithoutFilter");

    private RejectTemporalJoinInBatchRule(RelOptRuleOperand operand, String description) {
        super(operand, description);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        throw new TableException(MESSAGE);
    }
}
