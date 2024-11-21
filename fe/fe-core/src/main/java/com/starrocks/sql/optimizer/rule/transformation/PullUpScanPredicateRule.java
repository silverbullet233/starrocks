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

package com.starrocks.sql.optimizer.rule.transformation;

import com.google.common.collect.Lists;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.OperatorBuilderFactory;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalFilterOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalScanOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.TableScanPredicateExtractor;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.List;
import java.util.Objects;

public class PullUpScanPredicateRule extends TransformationRule {
    public static final PullUpScanPredicateRule OLAP_SCAN = new PullUpScanPredicateRule(OperatorType.LOGICAL_OLAP_SCAN);

    public PullUpScanPredicateRule(OperatorType type) {
        super(RuleType.TF_PULL_UP_PREDICATE_SCAN, Pattern.create(type));
    }

    @Override
    public boolean check(final OptExpression input, OptimizerContext context) {
        ScalarOperator predicates = input.getOp().getPredicate();
        if (!context.getSessionVariable().isEnableScanPredicateExprReuse() || predicates == null) {
            return false;
        }
        return true;
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalScanOperator logicalScanOperator = (LogicalScanOperator) input.getOp();
        ScalarOperator predicates = logicalScanOperator.getPredicate();
        TableScanPredicateExtractor tableScanPredicateExtractor =
                new TableScanPredicateExtractor(logicalScanOperator.getColRefToColumnMetaMap());
        tableScanPredicateExtractor.extract(predicates);
        ScalarOperator pushedPredicates = tableScanPredicateExtractor.getPushedPredicates();
        ScalarOperator reservedPredicates = tableScanPredicateExtractor.getReservedPredicates();
        boolean newScanPredicateIsSame = Objects.equals(pushedPredicates, predicates);
        if (newScanPredicateIsSame || reservedPredicates == null) {
            return Lists.newArrayList();
        }
        // generate Scan-> Filter
        Operator.Builder builder = OperatorBuilderFactory.build(logicalScanOperator);
        // @TODO scan operator's projection and output column may be changed
        LogicalScanOperator newScanOperator = (LogicalScanOperator) builder.withOperator(logicalScanOperator)
                .setPredicate(pushedPredicates).build();
        newScanOperator.buildColumnFilters(pushedPredicates);
        LogicalFilterOperator newFilterOperator = new LogicalFilterOperator(reservedPredicates);
        OptExpression filter = OptExpression.create(newFilterOperator, OptExpression.create(newScanOperator));
        return Lists.newArrayList(filter);
    }
}
