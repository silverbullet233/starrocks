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
import com.starrocks.sql.optimizer.operator.physical.PhysicalHashJoinOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalJoinOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalLimitOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalLookUpOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalTopNOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
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
public class LateMaterializedColumnCollector {
    private static final Logger LOG = LogManager.getLogger(LateMaterializedColumnCollector.class);

    public static void collectLateMaterializedColumn(OptExpression root) {
    }

    public OptExpression rewrite(OptExpression root, OptimizerContext context) {
        LOG.info("before rewrite, plan : " + root.debugString());
        CollectorContext collectorContext = new CollectorContext();
        collectorContext.columnRefFactory = context.getColumnRefFactory();

        ColumnCollector columnCollector = new ColumnCollector();
        root.getOp().accept(columnCollector, root, collectorContext);
        LOG.info("after rewrite, " + collectorContext);
        adjustFetchPositions(collectorContext);
        LOG.info("after adjust, " + collectorContext);

        OptExpression newRoot = rewritePlan(root, context, collectorContext);
        LOG.info("after PlanRewriter, " + newRoot.debugString());
        // @TODO we should refine LogicalProperty
        return newRoot;
    }

    private OptExpression rewritePlan(OptExpression root, OptimizerContext optimizerContext, CollectorContext collectorContext) {
        PlanRewriter rewriter = new PlanRewriter(optimizerContext, collectorContext);

        RewriteContext rewriteContext = new RewriteContext();
        // @TODO need deep clone
        collectorContext.unMaterializedColumns.entrySet().forEach(entry -> {
            rewriteContext.unFetchedColumns.put(entry.getKey(), new HashSet<>());
            rewriteContext.unFetchedColumns.get(entry.getKey()).addAll(entry.getValue());
        });

        OptExpression newRoot = root.getOp().accept(rewriter, root, rewriteContext);
        // @TODO consider un-materialized column
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
            {
                StringBuilder sb = new StringBuilder();
                sb.append("paths [");
                for (PhysicalOperator op : paths) {
                    sb.append(op).append(" -> ");
                }
                sb.append("]");
                LOG.info(sb);
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
        // Set<ColumnRefOperator> unMaterializedColumns = new HashSet<>();
        Map<PhysicalOlapScanOperator, Set<ColumnRefOperator>> unMaterializedColumns = new HashMap<>();

        Map<ColumnRefOperator, PhysicalOlapScanOperator> columnSources = new HashMap<>();

        // key -> column ref list
        // should add FetchOperator under key
        // Map<PhysicalOperator, Map<PhysicalOlapScanOperator, Set<ColumnRefOperator>>> fetchPositions = new HashMap<>();
        com.google.common.collect.Table<PhysicalOperator, PhysicalOlapScanOperator, Set<ColumnRefOperator>>
                fetchPositions = HashBasedTable.create();

        // used to record all olap scan operators that have late-materialized columns
        Set<PhysicalOlapScanOperator> needLookupSources = new HashSet<>();

        // @TODO should know if data will reduce between two operators
        // @TODo should maintain a path between scan and root, this will be used to adjust fetch operator position
        // use this to find path to root?
        Map<PhysicalOperator, PhysicalOperator> parents = new HashMap<>();

        // already fetched columns
        Set<ColumnRefOperator> fetchedColumns = new HashSet<>();

        // @TODO for a scan operator, we may fetch columns in multi places, we should know when we don't need rowid anymore

        // @TODO record rowid column ref for each table
        Map<PhysicalOlapScanOperator, ColumnRefOperator> rowIdColumns = new HashMap();
        // @TODO pass from Optimizer
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

    // bottom-up collector
    public static class ColumnCollector extends OptExpressionVisitor<Void, CollectorContext> {


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

        @Override
        public Void visit(OptExpression optExpression, CollectorContext context) {
            for (OptExpression input : optExpression.getInputs()) {
                input.getOp().accept(this, input,  context);
                context.parents.put((PhysicalOperator) input.getOp(), (PhysicalOperator) optExpression.getOp());
            }
            return null;
        }

        @Override
        public Void visitPhysicalOlapScan(OptExpression optExpression, CollectorContext context) {
            LOG.info("visitPhysicalOlapScan " + optExpression.getOp());
            // @TODO only handle duplicate key and primary key table
            // find all read column, record which table from
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
                // @TODO consider predicate and runtime filter??
            }
            return null;
        }

        @Override
        public Void visitPhysicalProject(OptExpression optExpression, CollectorContext context) {
            return visit(optExpression, context);
        }

        @Override
        public Void visitPhysicalHashAggregate(OptExpression optExpression, CollectorContext context) {
            return visit(optExpression, context);
        }

        @Override
        public Void visitPhysicalTopN(OptExpression optExpression, CollectorContext context) {
            visit(optExpression, context);
            PhysicalTopNOperator topNOperator = (PhysicalTopNOperator) optExpression.getOp();
            // @TODO should ignore window operator??
            List<Ordering> orderings = topNOperator.getOrderSpec().getOrderDescs();
            for (Ordering ordering : orderings) {
                ColumnRefOperator columnRefOperator = ordering.getColumnRef();
                if (!context.columnSources.containsKey(columnRefOperator)) {
                    continue;
                }
                materializedBefore(columnRefOperator, topNOperator, context);
            }
            return null;
        }

        @Override
        public Void visitPhysicalDistribution(OptExpression optExpression, CollectorContext context) {
            visit(optExpression, context);
            // @TODO collect required column
            PhysicalDistributionOperator distributionOperator = (PhysicalDistributionOperator) optExpression.getOp();
            // @TODO shuffle key column is must required
            DistributionSpec distributionSpec = distributionOperator.getDistributionSpec();
            // handle different sepc
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
            visit(optExpression, context);
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
            // @TODO remove projection columns?
            return null;
        }

        @Override
        public Void visitPhysicalNestLoopJoin(OptExpression optExpression, CollectorContext context) {
            return visit(optExpression, context);
        }

        @Override
        public Void visitPhysicalFilter(OptExpression optExpression, CollectorContext context) {
            return visit(optExpression, context);
        }

        @Override
        public Void visitPhysicalDecode(OptExpression optExpression, CollectorContext context) {
            return visit(optExpression, context);
        }

        @Override
        public Void visitPhysicalStreamAgg(OptExpression optExpression, CollectorContext context) {
            return visit(optExpression, context);
        }
    }

    // top-down add FetchOperator

    public static class RewriteContext {
        // row id column of each olap scan operator
        Map<PhysicalOlapScanOperator, ColumnRefOperator> rowIdColumns = new HashMap<>();
        // un-fetched columns
        Map<PhysicalOlapScanOperator, Set<ColumnRefOperator>> unFetchedColumns = new HashMap<>();

        // already fetchd columns
        Set<ColumnRefOperator> fetchedColumns = new HashSet<>();

        public RewriteContext() {

        }
    }

    // rewrite OptExpression, insert PhysicalFetchOperator

    // what will do:
    // 1. add row_id column in OlapScanOperator
    // 2. update projection and logical property if necessary
    // 3. update RowOutputInfo
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
                ctx.unFetchedColumns = context.unFetchedColumns;
                inputs.add(input.getOp().accept(this, input, ctx));
                // merge ctx
                context.rowIdColumns.putAll(ctx.rowIdColumns);
                context.fetchedColumns.addAll(ctx.fetchedColumns);
            }
            return inputs;
        }

        public void updateProjection(OptExpression optExpression, RewriteContext context) {
            PhysicalOperator op = (PhysicalOperator) optExpression.getOp();
            Projection projection = op.getProjection();
            if (projection != null) {
                // we should remove un fetched column
                projection.getColumnRefMap().entrySet().removeIf(entry -> {
                    ColumnRefOperator columnRef = entry.getKey();
                    // @TODO if a column is not fetched and it is not required, we can remove it
                    // @TODO if this column is required by its parent, we should keepit , but how?
                    if (!context.fetchedColumns.contains(columnRef)) {
                        // @TODO
                        LOG.info("remove column " + columnRef + " from operator: " + op);
                        return true;
                    }
                    return false;
                });

                // @TODO add rowid to projection
                context.rowIdColumns.values().forEach(columnRef -> {
                    projection.getColumnRefMap().put(columnRef, columnRef);
                    LOG.info("add rowid column " + columnRef + " to operator: " + op);
                });
            }
            // @TODO update logical property
            LogicalProperty logicalProperty = optExpression.getLogicalProperty();
            ColumnRefFactory columnRefFactory = optimizerContext.getColumnRefFactory();
            List<ColumnRefOperator> outputColumns
                    = logicalProperty.getOutputColumns().getColumnRefOperators(columnRefFactory);
            // 1. remove unfetched column
            List<ColumnRefOperator> newOutputColumns = outputColumns.stream().filter(columnRefOperator -> {
                if (!columnRefFactory.getColumnRefToColumns().containsKey(columnRefOperator)) {
                    // not original column, should keep
                    return true;
                }
                // not fetched, should remove
                // @TODO should consider must have column
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
            // 1. remove un fetched columns
        }

        @Override
        public OptExpression visit(OptExpression optExpression, RewriteContext context) {
            // @TODO insert physical fetch operator
            List<OptExpression> inputs = visitChildren(optExpression, context);
            // @TODO adjust projection?
            updateProjection(optExpression, context);
            // @TODO how to add ROW_ID column into projection
            LogicalProperty logicalProperty = optExpression.getLogicalProperty();
            // optExpression.deriveLogicalPropertyItself();
            LOG.info("operator + " + optExpression.getOp() + ", logical properties: " + logicalProperty.getOutputColumns());
            // insert FetchOperator
            PhysicalOperator physicalOperator = (PhysicalOperator) optExpression.getOp();
            if (collectorContext.fetchPositions.containsRow(physicalOperator)) {
                Map<PhysicalOlapScanOperator, Set<ColumnRefOperator>> columns =
                        collectorContext.fetchPositions.row(physicalOperator);

                // @TODO change child's projection?
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
                        // @TODO remove late-materialized column from projection
                    }
                });
                // @TODO we should update logical properties of fetch
                context.fetchedColumns.addAll(columnRefOperatorColumnMap.keySet());

                // @TODO rowid column of each table
                PhysicalFetchOperator physicalFetchOperator =
                        new PhysicalFetchOperator(tableColumns, columnRefOperatorColumnMap, rowidColumns);
                // @TODO create lookup
                PhysicalLookUpOperator physicalLookUpOperator =
                        new PhysicalLookUpOperator(tableColumns, columnRefOperatorColumnMap, rowidColumns);
                // create new OptExpression
                inputs.add(OptExpression.create(physicalLookUpOperator));
                OptExpression child = OptExpression.create(physicalFetchOperator, inputs);
                LogicalProperty newLogicalProperty = new LogicalProperty(logicalProperty);
                newLogicalProperty.getOutputColumns().union(columnRefOperatorColumnMap.keySet());
                child.setLogicalProperty(new LogicalProperty(logicalProperty));
                child.setStatistics(optExpression.getStatistics());
                OptExpression result = OptExpression.builder().with(optExpression).setInputs(Arrays.asList(child)).build();
                return result;
            } else {
                OptExpression result = OptExpression.builder().with(optExpression).setInputs(inputs).build();
                return result;
            }
            // @TODO rewrite logical properties?
        }

        // @TODO should remove unused column


        @Override
        public OptExpression visitPhysicalOlapScan(OptExpression optExpression, RewriteContext context) {
            // @TODO only handle duplicate key and primary key table
            // find all read column, record which table from
            PhysicalOlapScanOperator scanOperator = (PhysicalOlapScanOperator) optExpression.getOp();
            if (!collectorContext.needLookupSources.contains(scanOperator)) {
                return optExpression;
            }

            // @TODO rewrite?
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
            // @TODO need row id column
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
