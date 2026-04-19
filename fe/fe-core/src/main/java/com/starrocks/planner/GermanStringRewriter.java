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

import com.starrocks.builtins.VectorizedGermanStringFunctionMap;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.ScalarFunction;
import com.starrocks.catalog.Table;
import com.starrocks.qe.SessionVariable;
import com.starrocks.sql.ast.OrderByElement;
import com.starrocks.sql.ast.expression.BinaryPredicate;
import com.starrocks.sql.ast.expression.CaseExpr;
import com.starrocks.sql.ast.expression.CastExpr;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.FunctionCallExpr;
import com.starrocks.sql.ast.expression.InPredicate;
import com.starrocks.sql.ast.expression.LiteralExpr;
import com.starrocks.sql.ast.expression.SlotRef;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.type.PrimitiveType;
import com.starrocks.type.ScalarType;
import com.starrocks.type.Type;
import com.starrocks.type.TypeFactory;
import com.starrocks.type.VarcharType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Late-stage plan rewriter that converts {@code TYPE_VARCHAR} to
 * {@code TYPE_GERMAN_STRING} on slots and expressions reachable from OLAP scan
 * outputs when {@code enable_german_string} is on.
 *
 * <p>Design principle: {@code TYPE_GERMAN_STRING} is a BE-only optimization
 * type — it never appears in SQL, DDL, or FE's {@code FunctionSet}. FE
 * resolves function overloads purely on VARCHAR. This pass performs a pure
 * substitution right before Thrift serialization:
 * <ol>
 *   <li>Flip slot and expression types from VARCHAR to GERMAN_STRING on
 *       everything reachable from OLAP scan outputs.</li>
 *   <li>For each {@link FunctionCallExpr} whose VARCHAR fn_id has a GS
 *       counterpart (see
 *       {@link VectorizedGermanStringFunctionMap}), swap the fn_id and
 *       rebuild the {@link ScalarFunction} signature with GERMAN_STRING
 *       arg/return types. Functions without a GS counterpart keep their
 *       VARCHAR fn_id; their subtree types are NOT flipped to GS.</li>
 * </ol>
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
 */
public final class GermanStringRewriter {

    private static final Logger LOG = LogManager.getLogger(GermanStringRewriter.class);

    // Per-query context: slots that host an aggregate VALUE (i.e. an
    // aggregateExprs[i] output, not a grouping key). Their types must stay
    // on whatever the BE aggregate actually writes to them, because unmapped
    // string aggregates (min / max / group_concat / count_distinct on
    // string) produce BinaryColumn at runtime; flipping the slot to GS
    // would make the downstream consumer read through a
    // ColumnViewer<TYPE_GERMAN_STRING> and crash. Grouping-key slots are
    // NOT in this set -- those may legitimately pass through as GS when
    // the grouping column came from a GS-rewritten scan output.
    private static final ThreadLocal<Set<SlotId>> AGG_VALUE_SLOTS =
            ThreadLocal.withInitial(Collections::emptySet);

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
        Set<SlotId> aggValueSlots = collectAggregateValueSlotIds(execPlan, descTbl);
        AGG_VALUE_SLOTS.set(aggValueSlots);
        try {
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
                rewriteFragment(fragment, descTbl);
            }
        } finally {
            AGG_VALUE_SLOTS.remove();
        }
    }

    /**
     * Collect every SlotId that hosts an aggregate VALUE (i.e. an
     * aggregateExprs[i] output) in any AggregationNode of the plan. Grouping
     * key slots are intentionally excluded -- they may keep the grouping
     * column's GS type.
     *
     * <p>The slot layout on the aggregate's output / intermediate tuples is
     * {@code [groupingExprs..., aggregateExprs...]}, so agg-value slots are
     * at indices {@code groupingCount + i}.
     */
    private static Set<SlotId> collectAggregateValueSlotIds(ExecPlan execPlan, DescriptorTable descTbl) {
        Set<SlotId> slotIds = new HashSet<>();
        if (descTbl == null) {
            return slotIds;
        }
        for (PlanFragment fragment : execPlan.getFragments()) {
            PlanNode root = fragment.getPlanRoot();
            if (root != null) {
                collectAggregateValueSlotIds(root, descTbl, slotIds);
            }
        }
        return slotIds;
    }

    private static void collectAggregateValueSlotIds(PlanNode node, DescriptorTable descTbl, Set<SlotId> slotIds) {
        if (node instanceof AggregationNode) {
            AggregateInfo aggInfo = ((AggregationNode) node).getAggInfo();
            if (aggInfo != null) {
                addAggValueSlotIds(aggInfo, descTbl.getTupleDesc(aggInfo.getOutputTupleId()), slotIds);
                addAggValueSlotIds(aggInfo, descTbl.getTupleDesc(aggInfo.getIntermediateTupleId()), slotIds);
            }
        }
        if (node instanceof ExchangeNode) {
            return;
        }
        for (PlanNode child : node.getChildren()) {
            collectAggregateValueSlotIds(child, descTbl, slotIds);
        }
    }

    private static void addAggValueSlotIds(AggregateInfo aggInfo, TupleDescriptor tupleDesc, Set<SlotId> slotIds) {
        if (tupleDesc == null) {
            return;
        }
        List<SlotDescriptor> slots = tupleDesc.getSlots();
        int groupingCount = aggInfo.getGroupingExprs() != null ? aggInfo.getGroupingExprs().size() : 0;
        List<FunctionCallExpr> aggregateExprs = aggInfo.getAggregateExprs();
        if (aggregateExprs == null) {
            return;
        }
        for (int i = 0; i < aggregateExprs.size(); ++i) {
            int slotIdx = groupingCount + i;
            if (slotIdx < slots.size()) {
                slotIds.add(slots.get(slotIdx).getId());
            }
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
        // Aggregate VALUE slots (min / max / group_concat / count_distinct
        // outputs) must stay on the BE aggregate's actual column type
        // (usually VARCHAR / BinaryColumn). Grouping-key slots are allowed
        // to keep their GS type because they pass through from the input.
        if (AGG_VALUE_SLOTS.get().contains(slot.getId())) {
            return;
        }
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
    private static void rewriteFragment(PlanFragment fragment, DescriptorTable descTbl) {
        if (fragment == null) {
            return;
        }
        if (fragment.getPlanRoot() != null) {
            walkPlanTree(fragment.getPlanRoot(), fragment, descTbl);
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
    private static void walkPlanTree(PlanNode node, PlanFragment fragment, DescriptorTable descTbl) {
        if (node == null) {
            return;
        }
        rewritePlanNodeExprs(node, descTbl);
        if (node instanceof ExchangeNode) {
            return;
        }
        for (PlanNode child : node.getChildren()) {
            walkPlanTree(child, fragment, descTbl);
        }
    }

    /**
     * Rewrite every expression owned by the given plan node. Generic Expr
     * lists are handled via {@link #rewriteExprList}; node-specific expression
     * containers (join conjuncts, agg/sort/project/analytic exprs) are
     * enumerated explicitly so we catch them without reflection.
     */
    private static void rewritePlanNodeExprs(PlanNode node, DescriptorTable descTbl) {
        rewriteExprList(node.getConjuncts());

        if (node instanceof ScanNode) {
            ScanNode scanNode = (ScanNode) node;
            // `heavy exprs` hold scan-side projections that ExprToThrift
            // serializes through `TPlanNodeCommon.heavy_exprs` (see
            // OlapScanNode.toThrift). These are not reachable through
            // getConjuncts() and must be rewritten explicitly so GS children
            // and fn_ids flow to the BE for scan-pushed expressions.
            rewriteExprMap(scanNode.getHeavyExprs());
        }
        if (node instanceof OlapScanNode) {
            OlapScanNode olapScanNode = (OlapScanNode) node;
            // Bucket exprs are serialized as `bucket_exprs` and feed into the
            // scan-side bucket-pruning predicates. Keep them aligned with
            // rewritten slot types.
            rewriteExprList(olapScanNode.getBucketExprs());
        }

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
     * see the rewritten child types; function fn_id substitution happens on
     * the parent only when the builtin has a GS counterpart.
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
            // type. A SlotRef may point to a SlotDescriptor that was not
            // reached via DescriptorTable.getTupleDescs() (e.g. exchange-local
            // slots synthesised during planning). When the descriptor belongs
            // to a rewritable tuple, make sure it has been rewritten before
            // reading its type, so SlotRef stays in sync.
            SlotRef slotRef = (SlotRef) expr;
            SlotDescriptor desc = slotRef.getDesc();
            if (desc != null) {
                if (desc.getParent() == null || isRewritableTuple(desc.getParent())) {
                    rewriteSlotDescriptor(desc);
                }
                Type descType = desc.getType();
                if (!expr.getType().equals(descType)) {
                    slotRef.setType(descType);
                }
            } else if (isScalarVarchar(expr.getType())) {
                expr.setType(rewriteType(expr.getType()));
            }
            return;
        }

        if (expr instanceof FunctionCallExpr) {
            swapGermanStringFunction((FunctionCallExpr) expr);
            return;
        }

        // BinaryPredicate / InPredicate are not FunctionCallExprs and have no
        // fn_id to swap; BE dispatches them on {@code child_type}. When one
        // side is already GERMAN_STRING (typically a SlotRef into a rewritten
        // tuple), make every sibling consistent with GS: literals get a pure
        // type flip (BE's literal.cpp TYPE_GERMAN_STRING branch materializes a
        // GermanStringColumn from the stored value with no conversion); any
        // other VARCHAR child (e.g. a constant-folded CastExpr) gets an
        // explicit CAST(... AS GERMAN_STRING) wrapper so the predicate
        // dispatcher sees uniformly-typed inputs.
        if (expr instanceof BinaryPredicate || expr instanceof InPredicate) {
            coerceSiblingsToGermanString(expr);
        }

        if (expr instanceof CaseExpr) {
            unifyCaseBranchTypes((CaseExpr) expr);
        }
    }

    /**
     * Unify the result type of a {@link CaseExpr} when some THEN/ELSE branches
     * have been rewritten to {@code GERMAN_STRING} and others are still
     * {@code VARCHAR}. BE's CaseExpr dispatcher requires all result branches
     * to share the same LogicalType: a branch returning GermanStringColumn
     * adjacent to one returning BinaryColumn crashes in the column-copy path.
     *
     * <p>When at least one result branch is {@code GERMAN_STRING}, flip
     * sibling VARCHAR {@link LiteralExpr} branches in place and wrap
     * non-literal VARCHAR branches in {@code CAST(... AS GERMAN_STRING)}.
     * The CaseExpr's own type is then set to {@code GERMAN_STRING}.
     *
     * <p>CaseExpr child layout (see CaseExpr docstring):
     * [caseExpr?, when1, then1, when2, then2, ..., whenN, thenN, elseExpr?]
     * Only THEN and ELSE branches contribute to the result type.
     */
    private static void unifyCaseBranchTypes(CaseExpr caseExpr) {
        List<Integer> resultIndices = collectCaseResultIndices(caseExpr);
        boolean resultHasGs = false;
        for (int idx : resultIndices) {
            if (isScalarGermanString(caseExpr.getChild(idx).getType())) {
                resultHasGs = true;
                break;
            }
        }
        if (resultHasGs) {
            for (int idx : resultIndices) {
                coerceCaseBranchToGermanString(caseExpr, idx);
            }
            if (isScalarVarchar(caseExpr.getType())) {
                caseExpr.setType(rewriteType(caseExpr.getType()));
            }
        }

        // If the leading CASE expr (children[0]) is GERMAN_STRING, the BE
        // dispatches the WHEN comparisons as TYPE_GERMAN_STRING (child_type
        // is taken from children[0]). The WHEN branches must therefore also
        // be GS-typed; BE viewers would otherwise mismatch.
        if (caseExpr.hasCaseExpr()
                && isScalarGermanString(caseExpr.getChild(0).getType())) {
            List<Integer> whenIndices = collectCaseWhenIndices(caseExpr);
            for (int idx : whenIndices) {
                coerceCaseBranchToGermanString(caseExpr, idx);
            }
        }
    }

    /**
     * If the CaseExpr child at {@code idx} is scalar VARCHAR, promote it to
     * GERMAN_STRING: LiteralExpr gets an in-place type flip, anything else
     * gets wrapped in CAST(... AS GERMAN_STRING).
     */
    private static void coerceCaseBranchToGermanString(CaseExpr caseExpr, int idx) {
        Expr branch = caseExpr.getChild(idx);
        Type t = branch.getType();
        if (!isScalarVarchar(t)) {
            return;
        }
        if (branch instanceof LiteralExpr) {
            branch.setType(rewriteType(t));
        } else {
            int length = ((ScalarType) t).getLength();
            Type gsType = TypeFactory.createGermanStringType(length);
            caseExpr.setChild(idx, new CastExpr(gsType, branch));
        }
    }

    /**
     * Indices of the THEN/ELSE result branches of a {@link CaseExpr}, computed
     * from the child layout {@code [caseExpr?, when1, then1, ..., elseExpr?]}.
     * WHEN conditions and the optional leading caseExpr are excluded because
     * they do not participate in the result type.
     */
    private static List<Integer> collectCaseResultIndices(CaseExpr caseExpr) {
        int start = caseExpr.hasCaseExpr() ? 1 : 0;
        int end = caseExpr.hasElseExpr() ? caseExpr.getChildren().size() - 1 : caseExpr.getChildren().size();
        List<Integer> indices = new ArrayList<>();
        for (int i = start + 1; i < end; i += 2) {
            indices.add(i);
        }
        if (caseExpr.hasElseExpr()) {
            indices.add(caseExpr.getChildren().size() - 1);
        }
        return indices;
    }

    /**
     * Indices of the WHEN comparison branches of a CASE expression that has a
     * leading case expr (form {@code CASE expr WHEN ... THEN ...}). WHEN
     * branches are compared against {@code children[0]} and must therefore
     * match its type in the BE dispatcher.
     */
    private static List<Integer> collectCaseWhenIndices(CaseExpr caseExpr) {
        int start = caseExpr.hasCaseExpr() ? 1 : 0;
        int end = caseExpr.hasElseExpr() ? caseExpr.getChildren().size() - 1 : caseExpr.getChildren().size();
        List<Integer> indices = new ArrayList<>();
        for (int i = start; i < end; i += 2) {
            indices.add(i);
        }
        return indices;
    }

    /**
     * Swap a {@link FunctionCallExpr}'s VARCHAR builtin fn_id to its
     * GERMAN_STRING counterpart when one is registered in
     * {@link VectorizedGermanStringFunctionMap}. Also rebuild the
     * {@link ScalarFunction} with GERMAN_STRING arg/return types so the
     * resulting {@code TFunction} carries GS types end-to-end on the wire.
     *
     * <p>If no GS counterpart exists, the call is left on the VARCHAR fn_id
     * and the expression's return type is NOT flipped to GS. Callers of this
     * expression are expected to see a VARCHAR result; downstream consumers
     * built on {@link SlotRef}s bound to rewritten SlotDescriptors will still
     * advertise GS, which is fine because the column plumbing is column-type
     * driven at runtime.
     */
    private static void swapGermanStringFunction(FunctionCallExpr fnCall) {
        Function fn = fnCall.getFn();
        if (fn == null) {
            return;
        }
        // Idempotency: the plan can reference the same FunctionCallExpr via
        // multiple AggregateInfo lists (aggregateExprs / intermediateAggrExprs /
        // materializedAggregateExprs), so this method may run twice on the
        // same node. An already-swapped GS fn_id (38xxx) must be treated as
        // "already GS-native": skip both the swap and the VARCHAR fallback
        // that would otherwise wrap GS children in CAST(... AS VARCHAR).
        if (isGermanStringFunctionId(fn.getFunctionId())) {
            return;
        }
        Long gsFnId = VectorizedGermanStringFunctionMap.getGermanStringId(fn.getFunctionId());
        if (gsFnId != null) {
            String fnName = fn.getFunctionName().getFunction();
            Type[] originalArgs = fn.getArgs();
            List<Type> newArgTypes = new ArrayList<>(originalArgs == null ? 0 : originalArgs.length);
            if (originalArgs != null) {
                for (Type t : originalArgs) {
                    newArgTypes.add(rewriteStringType(t));
                }
            }
            Type newRetType = rewriteStringType(fn.getReturnType());

            ScalarFunction gsFn = ScalarFunction.createVectorizedBuiltin(
                    gsFnId, fnName, newArgTypes, fn.hasVarArgs(), newRetType);
            fnCall.setFn(gsFn);
            fnCall.setType(newRetType);

            // Every child of a GS-mapped function must produce a
            // GermanStringColumn at runtime, otherwise the GS viewer hits a
            // BinaryColumn and crashes. Flip VARCHAR literals in place (BE's
            // literal.cpp materialises a GermanStringColumn) and flip
            // CastExpr targets from VARCHAR to GS (BE's cast_expr routes the
            // corresponding GS variant). Any other non-GS child — rare, but
            // e.g. a FunctionCallExpr with no GS variant — gets wrapped in
            // CAST(... AS GERMAN_STRING) so the evaluation still produces a
            // GermanStringColumn at the GS builtin's boundary.
            for (int i = 0; i < fnCall.getChildren().size(); ++i) {
                Expr child = fnCall.getChild(i);
                if (!isScalarVarchar(child.getType())) {
                    continue;
                }
                if (child instanceof LiteralExpr) {
                    child.setType(rewriteType(child.getType()));
                } else if (child instanceof CastExpr) {
                    child.setType(rewriteType(child.getType()));
                } else {
                    int length = ((ScalarType) child.getType()).getLength();
                    Type gsType = TypeFactory.createGermanStringType(length);
                    fnCall.setChild(i, new CastExpr(gsType, child));
                }
            }
            return;
        }

        // No GS counterpart: the function keeps its VARCHAR fn_id. Any GS
        // children now flowing in (e.g. SlotRef into a rewritten slot) must
        // be converted back to VARCHAR because BE's VARCHAR builtin expects
        // BinaryColumn input. This is the single compatibility shim for
        // string builtins that lack a GS variant yet.
        for (int i = 0; i < fnCall.getChildren().size(); ++i) {
            Expr child = fnCall.getChild(i);
            if (!isScalarGermanString(child.getType())) {
                continue;
            }
            int length = ((ScalarType) child.getType()).getLength();
            Type varchar = length > 0 ? TypeFactory.createVarcharType(length) : VarcharType.VARCHAR;
            fnCall.setChild(i, new CastExpr(varchar, child));
        }
    }

    /**
     * If any child of {@code expr} is already {@code GERMAN_STRING}, make
     * every sibling consistent:
     * <ul>
     *   <li>{@link LiteralExpr} with VARCHAR type → in-place type flip to GS
     *       (BE's literal.cpp materializes a GermanStringColumn directly).</li>
     *   <li>Any other child with VARCHAR type (CastExpr, FunctionCallExpr with
     *       no GS variant, ...) → wrap in {@code CAST(... AS GERMAN_STRING)}
     *       so BE's predicate dispatcher sees a uniform child_type.</li>
     * </ul>
     */
    private static void coerceSiblingsToGermanString(Expr expr) {
        boolean hasGermanString = false;
        for (Expr child : expr.getChildren()) {
            if (isScalarGermanString(child.getType())) {
                hasGermanString = true;
                break;
            }
        }
        if (!hasGermanString) {
            return;
        }
        for (int i = 0; i < expr.getChildren().size(); ++i) {
            Expr child = expr.getChild(i);
            Type t = child.getType();
            if (!isScalarVarchar(t)) {
                continue;
            }
            if (child instanceof LiteralExpr) {
                child.setType(rewriteType(t));
            } else {
                int length = ((ScalarType) t).getLength();
                Type gsType = TypeFactory.createGermanStringType(length);
                expr.setChild(i, new CastExpr(gsType, child));
            }
        }
    }

    private static boolean isScalarGermanString(Type t) {
        return t != null && t.isScalarType()
                && t.getPrimitiveType() == PrimitiveType.GERMAN_STRING;
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

    /**
     * Returns a GERMAN_STRING replacement for VARCHAR/CHAR scalar types; any
     * other type (INT, BIGINT, BOOLEAN, already-GS, ...) is returned as-is.
     * Used when reconstructing a {@link ScalarFunction}'s signature from the
     * VARCHAR builtin.
     */
    private static Type rewriteStringType(Type t) {
        if (t == null || !t.isScalarType()) {
            return t;
        }
        PrimitiveType pt = t.getPrimitiveType();
        if (pt == PrimitiveType.VARCHAR || pt == PrimitiveType.CHAR) {
            int length = ((ScalarType) t).getLength();
            return TypeFactory.createGermanStringType(length);
        }
        return t;
    }

    private static boolean isScalarVarchar(Type t) {
        return t != null && t.isScalarType() && t.getPrimitiveType() == PrimitiveType.VARCHAR;
    }

    /**
     * Returns {@code true} if {@code fnId} is in the reserved GERMAN_STRING
     * builtin id range (3800x). GS fn_ids are generated by
     * {@code gensrc/script/functions.py} and are used as substitution targets
     * by {@link VectorizedGermanStringFunctionMap}; a FunctionCallExpr
     * carrying one has already been rewritten and should not be revisited.
     */
    private static boolean isGermanStringFunctionId(long fnId) {
        return fnId >= 38000L && fnId < 39000L;
    }
}
