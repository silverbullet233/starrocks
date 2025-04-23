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

package com.starrocks.sql.optimizer;

import com.google.api.client.util.Lists;
import com.google.common.base.Preconditions;
import com.google.common.collect.HashBasedTable;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.KeysType;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Type;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.base.DistributionCol;
import com.starrocks.sql.optimizer.base.DistributionSpec;
import com.starrocks.sql.optimizer.base.HashDistributionSpec;
import com.starrocks.sql.optimizer.base.LogicalProperty;
import com.starrocks.sql.optimizer.base.Ordering;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.physical.PhysicalDistributionOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalFetchOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalFilterOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalHashAggregateOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalHashJoinOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalJoinOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalLimitOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalLookUpOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalProjectOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalTopNOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

// use this to collect all late materialized columns
public class LateMaterializationRewriter {
    private static final Logger LOG = LogManager.getLogger(LateMaterializationRewriter.class);

    public OptExpression rewrite(OptExpression root, OptimizerContext context) {
        LOG.info("before rewrite, plan : " + root.debugString());
        CollectorContext collectorContext = new CollectorContext();
        collectorContext.columnRefFactory = context.getColumnRefFactory();

        ColumnCollector columnCollector = new ColumnCollector(context);
        columnCollector.visit(root, collectorContext);
        LOG.info("after rewrite, " + collectorContext);
        adjustFetchPositions(collectorContext);
        LOG.info("after adjust, " + collectorContext);

        OptExpression newRoot = rewritePlan(root, context, collectorContext);
        LOG.info("after PlanRewriter, " + newRoot.debugString());
        return newRoot;
    }

    private OptExpression rewritePlan(OptExpression root, OptimizerContext optimizerContext, CollectorContext collectorContext) {
        PlanRewriter rewriter = new PlanRewriter(optimizerContext, collectorContext);

        RewriteContext rewriteContext = new RewriteContext();

        OptExpression newRoot = root.getOp().accept(rewriter, root, rewriteContext);
        if (!collectorContext.unMaterializedColumns.isEmpty()) {
            // if there are still un-materialized columns, should add a fetch at the top of root
            Map<PhysicalOlapScanOperator, Set<ColumnRefOperator>> columns = collectorContext.unMaterializedColumns;
            Map<Table, Set<ColumnRefOperator>> tableColumns = new HashMap<>();
            Map<ColumnRefOperator, Column> columnRefOperatorColumnMap = new HashMap<>();
            Map<Table, ColumnRefOperator> rowidColumns = new HashMap<>();

            columns.forEach((scanOperator, columnRefs) -> {
                Table table = scanOperator.getTable();
                tableColumns.put(table, columnRefs);
                rowidColumns.put(table, rewriteContext.rowIdColumns.get(scanOperator));
                Map<ColumnRefOperator, Column> columnRefMap = scanOperator.getColRefToColumnMetaMap();
                for (ColumnRefOperator columnRef : columnRefs) {
                    columnRefOperatorColumnMap.put(columnRef, columnRefMap.get(columnRef));
                }
            });

            PhysicalFetchOperator physicalFetchOperator =
                    new PhysicalFetchOperator(tableColumns, columnRefOperatorColumnMap, rowidColumns);
            PhysicalLookUpOperator physicalLookUpOperator =
                    new PhysicalLookUpOperator(tableColumns, columnRefOperatorColumnMap, rowidColumns);
            // create new OptExpression
            OptExpression result = OptExpression.create(physicalFetchOperator, newRoot);
            result.setStatistics(newRoot.getStatistics());
            result.getInputs().add(OptExpression.create(physicalLookUpOperator));
            return result;
        }
        return newRoot;
    }


    private void adjustFetchPositions(CollectorContext context) {
        if (context.needLookupSources.isEmpty()) {
            return;
        }
        // build path
        for (PhysicalOlapScanOperator scanOperator : context.needLookupSources) {
            List<PhysicalOperator> paths = new ArrayList<>();
            paths.add(scanOperator);
            {
                PhysicalOperator current = scanOperator;
                while (context.parents.containsKey(current)) {
                    current = context.parents.get(current);
                    paths.add(current);
                }
            }
            // iterate paths, try to push lookup
            for (int i = 0; i < paths.size(); i++) {
                PhysicalOperator operator = paths.get(i);
                if (context.fetchPositions.contains(operator, scanOperator)) {
                    // we should check if we can push
                    int idx = i;
                    for (int j = i - 1; j >= 0; j--) {
                        PhysicalOperator op = paths.get(j);
                        // if there is an operator that can filter data, we can't push it
                        if (op instanceof PhysicalJoinOperator || op instanceof PhysicalTopNOperator
                                || op instanceof PhysicalFilterOperator || op instanceof PhysicalLimitOperator) {
                            break;
                        }
                        if (op instanceof PhysicalDistributionOperator || op instanceof PhysicalOlapScanOperator) {
                            idx = j;
                        }
                    }
                    if (idx < i) {
                        // we can move fetch to operator[idx]
                        PhysicalOperator targetParent = paths.get(idx);
                        Set<ColumnRefOperator> columnRefOperators = context.fetchPositions.get(operator, scanOperator);
                        context.fetchPositions.remove(operator, scanOperator);
                        if (!context.fetchPositions.contains(targetParent, scanOperator)) {
                            context.fetchPositions.put(targetParent, scanOperator, new HashSet<>());
                        }
                        context.fetchPositions.get(targetParent, scanOperator).addAll(columnRefOperators);
                    }

                }
            }
        }
    }

    public static class CollectorContext {
        // for each column, record the first-used position
        Map<ColumnRefOperator, PhysicalOperator> materializedPosition = new HashMap<>();
        // un-materialized columns
        Map<PhysicalOlapScanOperator, Set<ColumnRefOperator>> unMaterializedColumns = new HashMap<>();
        // which ScanOperator the column comes from
        Map<ColumnRefOperator, PhysicalOlapScanOperator> columnSources = new HashMap<>();

        // used to record the fetch position. for example,
        // a row in the table (A, B, [C,D]) indicates that
        // the Column C and D from PhysicalOlapScan B should be fetched before PhysicalOperator A
        com.google.common.collect.Table<PhysicalOperator, PhysicalOlapScanOperator, Set<ColumnRefOperator>>
                fetchPositions = HashBasedTable.create();

        // for operator with projection, we may fetch data before projection
        com.google.common.collect.Table<PhysicalOperator, PhysicalOlapScanOperator, Set<ColumnRefOperator>>
                fetchBeforeProjections = HashBasedTable.create();

        // used to record all olap scan operators that have late-materialized columns
        Set<PhysicalOlapScanOperator> needLookupSources = new HashSet<>();

        // use this to find path to root
        Map<PhysicalOperator, PhysicalOperator> parents = new HashMap<>();

        ColumnRefFactory columnRefFactory;

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("fetchPositions size " + fetchPositions.size()).append("\n");
            for (PhysicalOperator operator : fetchPositions.rowKeySet()) {
                Map<PhysicalOlapScanOperator, Set<ColumnRefOperator>> columns = fetchPositions.row(operator);
                sb.append("parent operator [" + operator + "], materialized columns [");
                for (Map.Entry<PhysicalOlapScanOperator, Set<ColumnRefOperator>> tableColumns : columns.entrySet()) {
                    PhysicalOlapScanOperator olapScanOperator = tableColumns.getKey();
                    sb.append("table " + olapScanOperator.getTable().getId() + ": [");
                    for (ColumnRefOperator column : tableColumns.getValue()) {
                        sb.append(column.getId()).append(",");
                    }
                    sb.append("],");
                }
                sb.append("]\n");
            }
            sb.append("fetchBeforeProjection size " + fetchBeforeProjections.size()).append("\n");
            for (PhysicalOperator operator : fetchBeforeProjections.rowKeySet()) {
                Map<PhysicalOlapScanOperator, Set<ColumnRefOperator>> columns = fetchBeforeProjections.row(operator);
                sb.append("operator [" + operator + "], materialized columns [");
                for (Map.Entry<PhysicalOlapScanOperator, Set<ColumnRefOperator>> tableColumns : columns.entrySet()) {
                    PhysicalOlapScanOperator olapScanOperator = tableColumns.getKey();
                    sb.append("table " + olapScanOperator.getTable().getId() + ": [");
                    for (ColumnRefOperator column : tableColumns.getValue()) {
                        sb.append(column.getId()).append(",");
                    }
                    sb.append("],");
                }
                sb.append("]\n");
            }

            sb.append("un-materialized columns " + unMaterializedColumns.size() + " [");
            for (Map.Entry<PhysicalOlapScanOperator, Set<ColumnRefOperator>> entry : unMaterializedColumns.entrySet()) {
                PhysicalOlapScanOperator sourceOperator = entry.getKey();
                sb.append("table " + sourceOperator.getTable().getId() + ": [");
                for (ColumnRefOperator columnRefOperator : entry.getValue()) {
                    sb.append(columnRefOperator.getId()).append(",");
                }
                sb.append("]\n");
            }
            sb.append("]\n");
            return sb.toString();
        }
    }

    // Traverse the PlanTree from bottom to top,
    // find all the columns that can be lazy read and where they need to be materialized
    // @TODO: support all operators
    public static class ColumnCollector extends OptExpressionVisitor<Void, CollectorContext> {
        private OptimizerContext optimizerContext;

        public ColumnCollector(OptimizerContext optimizerContext) {
            this.optimizerContext = optimizerContext;
        }

        private void materializedBefore(ColumnRefOperator columnRefOperator,
                                        PhysicalOperator operator, CollectorContext context) {
            if (!context.materializedPosition.containsKey(columnRefOperator)) {
                context.materializedPosition.put(columnRefOperator, operator);
                PhysicalOlapScanOperator sourceOperator = context.columnSources.get(columnRefOperator);
                context.needLookupSources.add(sourceOperator);
                if (!context.fetchPositions.contains(operator, sourceOperator)) {
                    context.fetchPositions.put(operator, sourceOperator, new HashSet<>());
                }
                context.fetchPositions.get(operator, sourceOperator).add(columnRefOperator);
                if (context.unMaterializedColumns.containsKey(sourceOperator)) {
                    context.unMaterializedColumns.get(sourceOperator).remove(columnRefOperator);
                    if (context.unMaterializedColumns.get(sourceOperator).isEmpty()) {
                        context.unMaterializedColumns.remove(sourceOperator);
                    }

                }
                LOG.info("materialize column " + columnRefOperator + " before " + operator);
            }
        }

        private void materializedBeforeProjection(ColumnRefOperator columnRefOperator,
                                                  PhysicalOperator operator, CollectorContext context) {
            Preconditions.checkState(operator.getProjection() != null);
            if (!context.materializedPosition.containsKey(columnRefOperator)) {
                context.materializedPosition.put(columnRefOperator, operator);
                PhysicalOlapScanOperator sourceOperator = context.columnSources.get(columnRefOperator);
                context.needLookupSources.add(sourceOperator);
                if (!context.fetchBeforeProjections.contains(operator, sourceOperator)) {
                    context.fetchBeforeProjections.put(operator, sourceOperator, new HashSet<>());
                }
                context.fetchBeforeProjections.get(operator, sourceOperator).add(columnRefOperator);
                if (context.unMaterializedColumns.containsKey(sourceOperator)) {
                    context.unMaterializedColumns.get(sourceOperator).remove(columnRefOperator);
                    if (context.unMaterializedColumns.get(sourceOperator).isEmpty()) {
                        context.unMaterializedColumns.remove(sourceOperator);
                    }
                }
                LOG.info("materialize column " + columnRefOperator + " before projection " + operator);
            }
        }

        @Override
        public Void visit(OptExpression optExpression, CollectorContext context) {
            for (OptExpression input : optExpression.getInputs()) {
                visit(input, context);
                context.parents.put((PhysicalOperator) input.getOp(), (PhysicalOperator) optExpression.getOp());
            }
            optExpression.getOp().accept(this, optExpression, context);
            return null;
        }

        @Override
        public Void visitPhysicalOlapScan(OptExpression optExpression, CollectorContext context) {
            LOG.info("visitPhysicalOlapScan " + optExpression.getOp());

            PhysicalOlapScanOperator scanOperator = (PhysicalOlapScanOperator) optExpression.getOp();
            OlapTable scanTable = (OlapTable) scanOperator.getTable();
            if (scanTable.getKeysType() == KeysType.DUP_KEYS || scanTable.getKeysType() == KeysType.PRIMARY_KEYS) {
                Map<ColumnRefOperator, Column> columnRefOperatorColumnMap = scanOperator.getColRefToColumnMetaMap();
                for (ColumnRefOperator columnRefOperator : columnRefOperatorColumnMap.keySet()) {
                    context.columnSources.put(columnRefOperator, scanOperator);
                    if (!context.unMaterializedColumns.containsKey(scanOperator)) {
                        context.unMaterializedColumns.put(scanOperator, new HashSet<>());
                    }
                    context.unMaterializedColumns.get(scanOperator).add(columnRefOperator);
                }
                if (scanOperator.getProjection() != null) {
                    Map<ColumnRefOperator, ScalarOperator> columnRefMap = scanOperator.getProjection().getColumnRefMap();
                    columnRefMap.forEach((columnRefOperator, scalarOperator) -> {
                        List<ColumnRefOperator> columnRefOperators = scalarOperator.getUsedColumns()
                                .getColumnRefOperators(optimizerContext.getColumnRefFactory());
                        columnRefOperators.forEach(op -> {
                            if (!context.columnSources.containsKey(op)) {
                                return;
                            }
                            materializedBefore(op, scanOperator, context);
                        });
                    });
                }
            }
            return null;
        }

        @Override
        public Void visitPhysicalHashAggregate(OptExpression optExpression, CollectorContext context) {
            // for aggregate operator, we should materialize all used columns
            PhysicalHashAggregateOperator aggregateOperator = (PhysicalHashAggregateOperator) optExpression.getOp();
            List<ColumnRefOperator> usedColumns =
                    aggregateOperator.getUsedColumns().getColumnRefOperators(optimizerContext.getColumnRefFactory());
            for (ColumnRefOperator columnRefOperator : usedColumns) {
                if (!context.columnSources.containsKey(columnRefOperator)) {
                    // not original column
                    continue;
                }
                materializedBefore(columnRefOperator, aggregateOperator, context);
            }
            return null;
        }

        @Override
        public Void visitPhysicalTopN(OptExpression optExpression, CollectorContext context) {
            PhysicalTopNOperator topNOperator = (PhysicalTopNOperator) optExpression.getOp();
            List<Ordering> orderings = topNOperator.getOrderSpec().getOrderDescs();
            for (Ordering ordering : orderings) {
                ColumnRefOperator columnRefOperator = ordering.getColumnRef();
                if (!context.columnSources.containsKey(columnRefOperator)) {
                    continue;
                }
                materializedBefore(columnRefOperator, topNOperator, context);
            }
            if (topNOperator.getProjection() != null) {
                // get all used columns
                Map<ColumnRefOperator, ScalarOperator> columnRefMap = topNOperator.getProjection().getColumnRefMap();
                columnRefMap.forEach((columnRefOperator, scalarOperator) -> {
                    List<ColumnRefOperator> columnRefOperators = scalarOperator.getUsedColumns()
                            .getColumnRefOperators(optimizerContext.getColumnRefFactory());
                    // we should fetch them
                    columnRefOperators.forEach(op -> {
                        if (!context.columnSources.containsKey(op)) {
                            return;
                        }
                        materializedBeforeProjection(op, topNOperator, context);
                    });
                });
            }
            return null;
        }

        @Override
        public Void visitPhysicalDistribution(OptExpression optExpression, CollectorContext context) {
            PhysicalDistributionOperator distributionOperator = (PhysicalDistributionOperator) optExpression.getOp();
            DistributionSpec distributionSpec = distributionOperator.getDistributionSpec();
            // handle different spec
            switch (distributionSpec.getType()) {
                case SHUFFLE: {
                    HashDistributionSpec hashDistributionSpec = (HashDistributionSpec) distributionSpec;
                    List<DistributionCol> shuffleColumns = hashDistributionSpec.getShuffleColumns();
                    // shuffle columns should be required
                    for (DistributionCol col : shuffleColumns) {
                        ColumnRefOperator columnRefOperator = context.columnRefFactory.getColumnRef(col.getColId());
                        if (!context.columnSources.containsKey(columnRefOperator)) {
                            LOG.info("column is not original column, skip handle it", columnRefOperator.getId());
                            continue;
                        }
                        materializedBefore(columnRefOperator, distributionOperator, context);
                    }
                    break;
                }
                default:
                    break;
            }

            return null;
        }

        @Override
        public Void visitPhysicalHashJoin(OptExpression optExpression, CollectorContext context) {
            PhysicalHashJoinOperator joinOperator = (PhysicalHashJoinOperator) optExpression.getOp();

            ColumnRefSet requiredColumns = joinOperator.getJoinConditionUsedColumns();
            List<ColumnRefOperator> columnRefOperators = requiredColumns.getColumnRefOperators(context.columnRefFactory);
            for (ColumnRefOperator columnRefOperator : columnRefOperators) {
                if (!context.columnSources.containsKey(columnRefOperator)) {
                    LOG.info("column " + columnRefOperator.getId() + " is not original column, skip");
                    continue;
                }
                materializedBefore(columnRefOperator, joinOperator, context);
            }
            return null;
        }
    }

    public static class RewriteContext {
        // row id column of each olap scan operator
        Map<PhysicalOlapScanOperator, ColumnRefOperator> rowIdColumns = new HashMap<>();
        // already fetched columns
        Set<ColumnRefOperator> fetchedColumns = new HashSet<>();

        public RewriteContext() {
        }
    }

    // rewrite PlanTree from bottom to top, what will do:
    // 1. add ROW_ID column in related OlapScanOperator;
    // 2. insert FetchOperator under the related Operator to read the required columns
    // 3. update Projection and LogicalProperty if necessary
    // @TODO support all operators
    public static class PlanRewriter extends OptExpressionVisitor<OptExpression, RewriteContext> {
        private OptimizerContext optimizerContext;
        private CollectorContext collectorContext;

        public PlanRewriter(OptimizerContext optimizerContext, CollectorContext collectorContext) {
            this.optimizerContext = optimizerContext;
            this.collectorContext = collectorContext;
        }

        private List<OptExpression> visitChildren(OptExpression optExpression, RewriteContext context) {
            List<OptExpression> inputs = Lists.newArrayList();
            for (OptExpression input : optExpression.getInputs()) {
                RewriteContext ctx = new RewriteContext();
                inputs.add(input.getOp().accept(this, input, ctx));
                // merge ctx
                context.rowIdColumns.putAll(ctx.rowIdColumns);
                context.fetchedColumns.addAll(ctx.fetchedColumns);
            }
            return inputs;
        }


        // if the Projection of an Operator need to lazy read some columns,
        // we split it to Operator(without Projection) -> FetchOperator -> ProjectOperator
        private OptExpression splitProjection(OptExpression optExpression, RewriteContext context) {
            PhysicalOperator op = (PhysicalOperator) optExpression.getOp();
            Projection projection = op.getProjection();
            if (collectorContext.fetchBeforeProjections.containsRow(op)) {
                Preconditions.checkState(projection != null);
                // remove projection from the original operator and create a new one
                PhysicalProjectOperator projectOperator = new PhysicalProjectOperator(
                        projection.getColumnRefMap(), projection.getCommonSubOperatorMap());

                Map<PhysicalOlapScanOperator, Set<ColumnRefOperator>> map1 =
                        new HashMap<>(collectorContext.fetchBeforeProjections.row(op));
                Map<PhysicalOlapScanOperator, Set<ColumnRefOperator>> map2 =
                        new HashMap<>(collectorContext.fetchPositions.row(op));
                map1.keySet().forEach(key -> {
                    collectorContext.fetchBeforeProjections.remove(op, key);
                });
                map2.keySet().forEach(key -> {
                    collectorContext.fetchPositions.remove(op, key);
                });

                op.setProjection(null);
                op.clearRowOutputInfo();

                collectorContext.fetchPositions.row(projectOperator).putAll(map1);

                RowOutputInfo newRowOutputInfo = optExpression.getRowOutputInfo();
                // we use this to update logical property
                LogicalProperty newLogicalProperty = new LogicalProperty(optExpression.getLogicalProperty());

                newLogicalProperty.setOutputColumns(newRowOutputInfo.getOutputColumnRefSet());

                optExpression.setLogicalProperty(newLogicalProperty);

                OptExpression result = OptExpression.create(projectOperator, optExpression);
                result.setLogicalProperty(optExpression.getLogicalProperty());
                result.setStatistics(optExpression.getStatistics());
                // @TODO remove unused columns from statistics?
                return result;
            }
            return optExpression;
        }

        public void updateProjection(OptExpression optExpression, RewriteContext context) {
            PhysicalOperator op = (PhysicalOperator) optExpression.getOp();
            Projection projection = op.getProjection();
            if (projection != null) {
                // 1. collect all column ref operators that only contain fetched column,
                Set<ColumnRefOperator> pendingRemovedColumnRefs = new HashSet<>();
                projection.getColumnRefMap().forEach(((columnRefOperator, scalarOperator) -> {
                    List<ColumnRefOperator> usedColumns =
                            scalarOperator.getUsedColumns().getColumnRefOperators(optimizerContext.getColumnRefFactory());
                    boolean allFetched = usedColumns.stream().allMatch(col -> {
                        if (!collectorContext.columnSources.containsKey(col)) {
                            // if not original column, ignore it
                            return true;
                        }
                        return context.fetchedColumns.contains(col);
                    });
                    // if not all columns are fetched, we should remove it from projection
                    if (!allFetched) {
                        pendingRemovedColumnRefs.add(columnRefOperator);
                    }
                }));

                Map<PhysicalOlapScanOperator, Set<ColumnRefOperator>> map1 = collectorContext.fetchPositions.row(op);

                projection.getColumnRefMap().entrySet().removeIf(entry -> {
                    ColumnRefOperator columnRef = entry.getKey();
                    if (pendingRemovedColumnRefs.contains(columnRef)) {
                        LOG.info("remove column " + columnRef + " from operator: " + op);
                        return true;
                    }
                    return false;
                });

                context.rowIdColumns.values().forEach(columnRef -> {
                    projection.getColumnRefMap().put(columnRef, columnRef);
                    LOG.info("add rowid column " + columnRef + " to operator: " + op);
                });
                collectorContext.fetchPositions.row(op).putAll(map1);
            }

            // update logical property
            LogicalProperty logicalProperty = optExpression.getLogicalProperty();
            ColumnRefFactory columnRefFactory = optimizerContext.getColumnRefFactory();
            List<ColumnRefOperator> outputColumns
                    = logicalProperty.getOutputColumns().getColumnRefOperators(columnRefFactory);
            List<ColumnRefOperator> newOutputColumns = outputColumns.stream().filter(columnRefOperator -> {
                if (!columnRefFactory.getColumnRefToColumns().containsKey(columnRefOperator)) {
                    // not original column, should keep
                    return true;
                }
                if (!context.fetchedColumns.contains(columnRefOperator)) {
                    LOG.info("remove column " + columnRefOperator + " from logical property of " + op);
                    return false;
                }
                return true;
            }).collect(Collectors.toList());
            // 2. add necessary row id
            context.rowIdColumns.values().forEach(entry -> {
                LOG.info("add row id column " + entry + " to logical property of " + op);
                newOutputColumns.add(entry);
            });
            logicalProperty.setOutputColumns(new ColumnRefSet(newOutputColumns));
        }

        @Override
        public OptExpression visit(OptExpression optExpression, RewriteContext context) {

            PhysicalOperator physicalOperator = (PhysicalOperator) optExpression.getOp();
            if (collectorContext.fetchBeforeProjections.containsRow(physicalOperator)) {
                OptExpression newRoot = splitProjection(optExpression, context);
                return newRoot.getOp().accept(this, newRoot, context);
            }

            LogicalProperty logicalProperty = optExpression.getLogicalProperty();
            List<OptExpression> inputs = visitChildren(optExpression, context);

            updateProjection(optExpression, context);

            LOG.info("operator + " + optExpression.getOp() + ", logical properties: " + logicalProperty.getOutputColumns());
            OptExpression result = null;
            if (collectorContext.fetchPositions.containsRow(physicalOperator)) {
                // if there are lazy-materialized columns, we should insert FetchOperator
                Map<PhysicalOlapScanOperator, Set<ColumnRefOperator>> columns =
                        collectorContext.fetchPositions.row(physicalOperator);

                Map<Table, Set<ColumnRefOperator>> tableColumns = new HashMap<>();
                Map<Table, ColumnRefOperator> rowidColumns = new HashMap<>();
                Map<ColumnRefOperator, Column> columnRefOperatorColumnMap = new HashMap<>();

                columns.forEach((scanOperator, columnRefs) -> {
                    Table table = scanOperator.getTable();
                    tableColumns.put(table, columnRefs);
                    rowidColumns.put(table, context.rowIdColumns.get(scanOperator));
                    Map<ColumnRefOperator, Column> columnRefMap = scanOperator.getColRefToColumnMetaMap();
                    for (ColumnRefOperator columnRef : columnRefs) {
                        columnRefOperatorColumnMap.put(columnRef, columnRefMap.get(columnRef));
                    }
                });
                // update fetched Columns
                context.fetchedColumns.addAll(columnRefOperatorColumnMap.keySet());

                PhysicalFetchOperator physicalFetchOperator =
                        new PhysicalFetchOperator(tableColumns, columnRefOperatorColumnMap, rowidColumns);
                PhysicalLookUpOperator physicalLookUpOperator =
                        new PhysicalLookUpOperator(tableColumns, columnRefOperatorColumnMap, rowidColumns);
                // create new OptExpression
                inputs.add(OptExpression.create(physicalLookUpOperator));
                OptExpression child = OptExpression.create(physicalFetchOperator, inputs);
                LogicalProperty newLogicalProperty = new LogicalProperty(logicalProperty);
                newLogicalProperty.getOutputColumns().union(columnRefOperatorColumnMap.keySet());
                child.setLogicalProperty(new LogicalProperty(logicalProperty));
                child.setStatistics(optExpression.getStatistics());

                result = OptExpression.builder().with(optExpression).setInputs(Arrays.asList(child)).build();
            }

            if (result == null) {
                result = OptExpression.builder().with(optExpression).setInputs(inputs).build();
            }
            return result;
        }

        @Override
        public OptExpression visitPhysicalOlapScan(OptExpression optExpression, RewriteContext context) {
            PhysicalOlapScanOperator scanOperator = (PhysicalOlapScanOperator) optExpression.getOp();
            if (!collectorContext.needLookupSources.contains(scanOperator)) {
                return optExpression;
            }

            Set<ColumnRefOperator> columnRefOperators = collectorContext.fetchPositions.get(scanOperator, scanOperator);
            if (columnRefOperators.size() == scanOperator.getColRefToColumnMetaMap().size()) {
                // all column need fetch, no need to rewrite
                context.fetchedColumns.addAll(columnRefOperators);
                return optExpression;
            }

            // modify output columns
            Map<ColumnRefOperator, Column> newColumnRefMap =
                    scanOperator.getColRefToColumnMetaMap().entrySet().stream()
                            .filter(entry -> columnRefOperators.contains(entry.getKey()))
                            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
            context.fetchedColumns.addAll(newColumnRefMap.keySet());
            // generate row id column
            Column rowIdColumn = new Column("ROW_ID", Type.ROW_ID, false);
            ColumnRefOperator columnRefOperator = optimizerContext.getColumnRefFactory().create("ROW_ID", Type.ROW_ID, false);
            newColumnRefMap.put(columnRefOperator, rowIdColumn);
            context.rowIdColumns.put(scanOperator, columnRefOperator);

            LOG.info("rewrite PhysicalOlapScan, newColumnRefMap: " + newColumnRefMap.size());
            // build a new optExpressions
            PhysicalOlapScanOperator.Builder builder = PhysicalOlapScanOperator.builder().withOperator(scanOperator);
            builder.setColRefToColumnMetaMap(newColumnRefMap);

            OptExpression result = OptExpression.builder().with(optExpression).setOp(builder.build()).build();
            LogicalProperty newProperty = new LogicalProperty(optExpression.getLogicalProperty());
            newProperty.setOutputColumns(new ColumnRefSet(newColumnRefMap.keySet()));
            result.setLogicalProperty(newProperty);
            LOG.info("operator + " + result.getOp() +
                    ", logical properties: " + result.getLogicalProperty().getOutputColumns());
            return result;
        }
    }

}
