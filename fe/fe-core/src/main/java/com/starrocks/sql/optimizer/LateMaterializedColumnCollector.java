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

import com.starrocks.sql.optimizer.operator.physical.PhysicalOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;

import java.util.Map;

// use this to collect all late materialized columns
public class LateMaterializedColumnCollector {

    public static void collectLateMaterializedColumn(OptExpression root) {

    }

    public static class CollectorContext {
        // for each column, record the first-used position
        Map<ColumnRefOperator, PhysicalOperator> materializedPosition;

        // where the column is from
        Map<ColumnRefOperator, PhysicalOperator> columnSource;
    }

    public static class ColumnCollector extends OptExpressionVisitor<Void, CollectorContext> {

        @Override
        public Void visit(OptExpression optExpression, CollectorContext context) {
            return null;
        }

        @Override
        public Void visitPhysicalOlapScan(OptExpression optExpression, CollectorContext context) {
            return null;
        }

        @Override
        public Void visitPhysicalProject(OptExpression optExpression, CollectorContext context) {
            return null;
        }

        @Override
        public Void visitPhysicalHashAggregate(OptExpression optExpression, CollectorContext context) {
            return null;
        }

        @Override
        public Void visitPhysicalTopN(OptExpression optExpression, CollectorContext context) {
            return null;
        }

        @Override
        public Void visitPhysicalDistribution(OptExpression optExpression, CollectorContext context) {
            return null;
        }

        @Override
        public Void visitPhysicalHashJoin(OptExpression optExpression, CollectorContext context) {
            return null;
        }

        @Override
        public Void visitPhysicalNestLoopJoin(OptExpression optExpression, CollectorContext context) {
            return null;
        }

        @Override
        public Void visitPhysicalFilter(OptExpression optExpression, CollectorContext context) {
            return null;
        }

        @Override
        public Void visitPhysicalDecode(OptExpression optExpression, CollectorContext context) {
            return null;
        }

        @Override
        public Void visitPhysicalStreamAgg(OptExpression optExpression, CollectorContext context) {
            return null;
        }
    }


}
