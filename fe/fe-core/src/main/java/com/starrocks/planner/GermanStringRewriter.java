// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.starrocks.planner;

import com.starrocks.catalog.Table;
import com.starrocks.qe.SessionVariable;
import com.starrocks.sql.ast.OrderByElement;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.SlotRef;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.type.PrimitiveType;
import com.starrocks.type.ScalarType;
import com.starrocks.type.Type;
import com.starrocks.type.TypeFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Late-stage plan rewriter that converts {@code TYPE_VARCHAR} to
 * {@code TYPE_GERMAN_STRING} on slots and expressions reachable from OLAP scan
 * outputs when {@code enable_german_string} is on.
 *
 * <p>Invariants preserved by this pass:
 * <ul>
 *   <li>Storage catalog (DDL) stays on VARCHAR -- this pass never mutates
 *       {@link com.starrocks.catalog.Column} metadata.</li>
 *   <li>MySQL wire column metadata stays on VARCHAR because the wire formatter
 *       uses {@link Expr#getOriginType()}, which we leave alone; only
 *       {@code Expr.type} is rewritten for BE transport.</li>
 *   <li>Slots belonging to external/connector scan tuples are skipped so
 *       those scans continue to hand BinaryColumn up the pipeline.</li>
 *   <li>Only scalar VARCHAR is rewritten. ARRAY/MAP/STRUCT with VARCHAR
 *       elements are left intact (out of scope for the first cut).</li>
 *   <li>CHAR is left as-is; only VARCHAR is converted.</li>
 * </ul>
 *
 * <p><b>Mutation model.</b> The pass mutates the {@code ExecPlan} in place.
 * Because plan objects are not reused across queries in this path, and because
 * the analyzer's cached {@code ScalarType.VARCHAR} singleton is never mutated
 * (we always replace references with a freshly constructed
 * {@link com.starrocks.type.GermanStringType}), the rewrite is safe for the
 * benchmark-oriented prototype. Do NOT invoke this pass on a shared/cached
 * plan object.
 */
public final class GermanStringRewriter {

    private GermanStringRewriter() {
    }

    /**
     * Returns {@code true} when the session has opted into the GermanString
     * query path. Callers should short-circuit the rewrite when this is false.
     */
    public static boolean shouldApply(SessionVariable sv) {
        return sv != null && sv.isEnableGermanString();
    }

    /**
     * Apply the VARCHAR -> GERMAN_STRING rewrite to every slot and expression
     * reachable from OLAP scan outputs in the given {@link ExecPlan}.
     *
     * <p>This method is a no-op if the plan or its descriptor table is null.
     */
    public static void rewrite(ExecPlan execPlan) {
        if (execPlan == null) {
            return;
        }
        DescriptorTable descTbl = execPlan.getDescTbl();
        if (descTbl != null) {
            for (TupleDescriptor tupleDesc : descTbl.getTupleDescs()) {
                if (!isRewritableTuple(tupleDesc)) {
                    continue;
                }
                for (SlotDescriptor slot : tupleDesc.getSlots()) {
                    rewriteSlotDescriptor(slot);
                }
            }
        }

        for (PlanFragment fragment : execPlan.getFragments()) {
            rewriteFragment(fragment);
        }
    }

    /**
     * A tuple is rewritable if it is not bound to a non-OLAP external table.
     * Intermediate tuples (null table) produced by agg/sort/join/project stages
     * downstream of an OlapScan are rewritable.
     */
    private static boolean isRewritableTuple(TupleDescriptor tupleDesc) {
        Table table = tupleDesc.getTable();
        if (table == null) {
            return true;
        }
        // External/connector tables (Hive/Iceberg/Hudi/JDBC/...) keep VARCHAR so
        // that their scan operators continue to produce BinaryColumn. Only
        // native OLAP/cloud-native tables participate in the rewrite.
        return table.isOlapOrCloudNativeTable();
    }

    /**
     * Update a slot's in-memory types so {@link SlotDescriptor#toThrift()}
     * emits {@code TYPE_GERMAN_STRING}. Both {@code type} and {@code originType}
     * are updated because {@link SlotDescriptor#toThrift()} prefers
     * {@code originType} when set.
     */
    private static void rewriteSlotDescriptor(SlotDescriptor slot) {
        Type newType = rewriteType(slot.getType());
        if (newType != slot.getType()) {
            slot.setType(newType);
        }
        Type originType = slot.getOriginType();
        if (originType != null && isScalarVarchar(originType)) {
            slot.setOriginType(rewriteType(originType));
        }
    }

    /**
     * Rewrite expressions reachable from the fragment. We intentionally avoid
     * mutating the {@code ExecPlan.outputExprs} list (used for MySQL wire
     * metadata); only {@link PlanFragment#getOutputExprs()} (a clone) is
     * rewritten so BE receives GERMAN_STRING while the client still sees
     * VARCHAR.
     */
    private static void rewriteFragment(PlanFragment fragment) {
        if (fragment == null) {
            return;
        }
        if (fragment.getPlanRoot() != null) {
            walkPlanTree(fragment.getPlanRoot(), fragment);
        }
        rewriteExprList(fragment.getOutputExprs());

        DataPartition dataPartition = fragment.getDataPartition();
        if (dataPartition != null) {
            rewriteExprList(dataPartition.getPartitionExprs());
        }
        DataPartition outputPartition = fragment.getOutputPartition();
        if (outputPartition != null && outputPartition != dataPartition) {
            rewriteExprList(outputPartition.getPartitionExprs());
        }

        Map<Integer, Expr> globalDictExprs = fragment.getQueryGlobalDictExprs();
        if (globalDictExprs != null) {
            for (Expr expr : globalDictExprs.values()) {
                rewriteExprTree(expr);
            }
        }

        DataSink sink = fragment.getSink();
        if (sink instanceof DataStreamSink) {
            DataStreamSink streamSink = (DataStreamSink) sink;
            DataPartition streamPartition = streamSink.getOutputPartition();
            if (streamPartition != null
                    && streamPartition != dataPartition
                    && streamPartition != outputPartition) {
                rewriteExprList(streamPartition.getPartitionExprs());
            }
        }
        // ResultSink carries no column metadata; MySQL wire types are reported
        // separately from ExecPlan.outputExprs. See class javadoc.
    }

    /**
     * Depth-first walk over the plan tree rooted at {@code node}, rewriting
     * expressions on each node and staying inside the owning {@code fragment}
     * (ExchangeNode children belong to upstream fragments and are handled
     * when those fragments are visited).
     */
    private static void walkPlanTree(PlanNode node, PlanFragment fragment) {
        if (node == null) {
            return;
        }
        rewritePlanNodeExprs(node);
        if (node instanceof ExchangeNode) {
            return;
        }
        for (PlanNode child : node.getChildren()) {
            walkPlanTree(child, fragment);
        }
    }

    /**
     * Rewrite every expression owned by the given plan node. Generic Expr
     * lists are handled via {@link #rewriteExprList}; node-specific expression
     * containers (join conjuncts, agg/sort/project/analytic exprs) are
     * enumerated explicitly so we catch them without reflection.
     */
    private static void rewritePlanNodeExprs(PlanNode node) {
        rewriteExprList(node.getConjuncts());

        if (node instanceof JoinNode) {
            JoinNode joinNode = (JoinNode) node;
            rewriteExprList(joinNode.getEqJoinConjuncts());
            // otherJoinConjuncts is package-private on JoinNode.
            rewriteExprList(joinNode.otherJoinConjuncts);
        }
        if (node instanceof AggregationNode) {
            AggregationNode aggNode = (AggregationNode) node;
            AggregateInfo aggInfo = aggNode.getAggInfo();
            if (aggInfo != null) {
                rewriteExprList(aggInfo.getGroupingExprs());
                rewriteExprList(aggInfo.getAggregateExprs());
                rewriteExprList(aggInfo.getMaterializedAggregateExprs());
                rewriteExprList(aggInfo.getIntermediateAggrExprs());
                rewriteExprList(aggInfo.getPartitionExprs());
            }
        }
        if (node instanceof SortNode) {
            SortNode sortNode = (SortNode) node;
            SortInfo sortInfo = sortNode.getSortInfo();
            if (sortInfo != null) {
                rewriteExprList(sortInfo.getOrderingExprs());
                if (sortInfo.getPartitionExprs() != null) {
                    rewriteExprList(sortInfo.getPartitionExprs());
                }
            }
        }
        if (node instanceof ProjectNode) {
            ProjectNode projectNode = (ProjectNode) node;
            rewriteExprMap(projectNode.getSlotMap());
            rewriteExprMap(projectNode.getCommonSlotMap());
        }
        if (node instanceof AnalyticEvalNode) {
            AnalyticEvalNode analyticNode = (AnalyticEvalNode) node;
            rewriteExprList(analyticNode.getAnalyticFnCalls());
            rewriteExprList(analyticNode.getPartitionExprs());
            if (analyticNode.getOrderByElements() != null) {
                for (OrderByElement element : analyticNode.getOrderByElements()) {
                    rewriteExprTree(element.getExpr());
                }
            }
        }
    }

    private static void rewriteExprMap(Map<?, Expr> map) {
        if (map == null) {
            return;
        }
        for (Expr expr : map.values()) {
            rewriteExprTree(expr);
        }
    }

    private static void rewriteExprList(Collection<? extends Expr> exprs) {
        if (exprs == null) {
            return;
        }
        // Snapshot to tolerate any incidental concurrent modification through
        // mutating setType paths on rare Expr subclasses.
        List<Expr> snapshot = new ArrayList<>(exprs);
        for (Expr expr : snapshot) {
            rewriteExprTree(expr);
        }
    }

    /**
     * Recursively rewrite every expression in the subtree rooted at
     * {@code expr}. Children are visited first so parent function signatures
     * see the rewritten child types, matching how cast/coercion exprs derive
     * their own type from child types at analysis time.
     */
    private static void rewriteExprTree(Expr expr) {
        if (expr == null) {
            return;
        }
        for (Expr child : expr.getChildren()) {
            rewriteExprTree(child);
        }

        if (expr instanceof SlotRef) {
            // Bind the SlotRef's type to its (possibly rewritten) descriptor
            // type. We call the overridden setType which also writes back to
            // desc, but that's idempotent: desc is already GERMAN_STRING for
            // rewritable tuples and still VARCHAR for external ones.
            SlotRef slotRef = (SlotRef) expr;
            if (slotRef.getDesc() != null) {
                Type descType = slotRef.getDesc().getType();
                if (!expr.getType().equals(descType)) {
                    slotRef.setType(descType);
                }
            } else if (isScalarVarchar(expr.getType())) {
                expr.setType(rewriteType(expr.getType()));
            }
            return;
        }

        if (isScalarVarchar(expr.getType())) {
            expr.setType(rewriteType(expr.getType()));
        }
    }

    /**
     * Returns a new GERMAN_STRING scalar type with the same length as the
     * source VARCHAR, or the input unchanged if it is not a scalar VARCHAR.
     */
    private static Type rewriteType(Type t) {
        if (!isScalarVarchar(t)) {
            return t;
        }
        ScalarType scalarType = (ScalarType) t;
        return TypeFactory.createGermanStringType(scalarType.getLength());
    }

    private static boolean isScalarVarchar(Type t) {
        return t != null && t.isScalarType() && t.getPrimitiveType() == PrimitiveType.VARCHAR;
    }
}
