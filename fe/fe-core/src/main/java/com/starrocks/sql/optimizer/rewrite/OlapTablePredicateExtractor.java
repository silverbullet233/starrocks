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

package com.starrocks.sql.optimizer.rewrite;

import com.starrocks.catalog.Column;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.scalar.ArrayOperator;
import com.starrocks.sql.optimizer.operator.scalar.ArraySliceOperator;
import com.starrocks.sql.optimizer.operator.scalar.BetweenPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.LambdaFunctionOperator;
import com.starrocks.sql.optimizer.operator.scalar.MapOperator;
import com.starrocks.sql.optimizer.operator.scalar.PredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperatorVisitor;
import com.starrocks.sql.optimizer.operator.scalar.SubfieldOperator;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

// extract olap table scan predicates to two parts: pushed-down to scan node, keep into filter node
public class OlapTablePredicateExtractor {
    private Map<ColumnRefOperator, Column> colRefToColumnMetaMap;
    private List<ScalarOperator> pushedPredicates = new LinkedList<>();
    private List<ScalarOperator> reservedPredicates = new LinkedList<>();

    public OlapTablePredicateExtractor(Map<ColumnRefOperator, Column> colRefToColumnMetaMap) {
        this.colRefToColumnMetaMap = colRefToColumnMetaMap;
    }

    public ScalarOperator getPushedPredicates() {
        return Utils.compoundAnd(pushedPredicates);
    }

    public ScalarOperator getReservedPredicates() {
        return Utils.compoundAnd(reservedPredicates);
    }

    public void extract(ScalarOperator op) {
        // @TODO pending fix
        // extract rule
        // 1. single column predicate can push down
        // 2. lambda function can't push down
        // 3. multi column predicate can't pushdown (e,g. a > b)
        // how about or??

        // (a > b) or (c = 1), we must push a > b to scan?
        // or ?

        if (op.getOpType().equals(OperatorType.COMPOUND)) {
            CompoundPredicateOperator operator = (CompoundPredicateOperator) op;
            switch (operator.getCompoundType()) {
                case AND: {
                    List<ScalarOperator> conjuncts = Utils.extractConjuncts(operator);
                    for (ScalarOperator conjunct : conjuncts) {
                        if (conjunct.accept(new CanFullyPushDownVisitor(), null)) {
                            pushedPredicates.add(conjunct);
                        } else {
                            reservedPredicates.add(conjunct);
                        }
                    }
                    return;
                }
                case OR: {
                    for (ScalarOperator child : operator.getChildren()) {
                        if (!child.accept(new CanFullyPushDownVisitor(), null)) {
                            reservedPredicates.add(op);
                            return;
                        }
                    }
                    pushedPredicates.add(op);
                    return;
                }
                case NOT: {
                    if (op.getChild(0).accept(new CanFullyPushDownVisitor(), null)) {
                        pushedPredicates.add(op);
                    } else {
                        reservedPredicates.add(op);
                    }
                    return;
                }
            }
            return;
        }
        if (op.accept(new CanFullyPushDownVisitor(), null)) {
            pushedPredicates.add(op);
        } else {
            reservedPredicates.add(op);
        }
    }

    private class CanFullyPushDownVisitor extends ScalarOperatorVisitor<Boolean, Void> {
        public CanFullyPushDownVisitor() {

        }

        private Boolean visitAllChildren(ScalarOperator op, Void context) {
            for (ScalarOperator child : op.getChildren()) {
                if (!child.accept(this, context)) {
                    return false;
                }
            }
            return true;
        }

        @Override
        public Boolean visit(ScalarOperator scalarOperator, Void context) {
            return false;
        }

        @Override
        public Boolean visitConstant(ConstantOperator op, Void context) {
            return true;
        }

        @Override
        public Boolean visitVariableReference(ColumnRefOperator op, Void context) {
            return true;
        }

        @Override
        public Boolean visitSubfield(SubfieldOperator subfieldOperator, Void context) {
            return true;
        }

        @Override
        public Boolean visitArray(ArrayOperator array, Void context) {
            return true;
        }

        @Override
        public Boolean visitMap(MapOperator map, Void context) {
            return true;
        }

        @Override
        public Boolean visitArraySlice(ArraySliceOperator op, Void context) {
            return true;
        }

        @Override
        public Boolean visitCall(CallOperator call, Void context) {
            return visitAllChildren(call, context);
        }

        @Override
        public Boolean visitPredicate(PredicateOperator op, Void context) {
            return visitAllChildren(op, context);
        }

        @Override
        public Boolean visitBetweenPredicate(BetweenPredicateOperator op, Void context) {
            return visitPredicate(op, context);
        }

        @Override
        public Boolean visitBinaryPredicate(BinaryPredicateOperator predicate, Void context) {
            return visitPredicate(predicate, context);
        }

        @Override
        public Boolean visitLambdaFunctionOperator(LambdaFunctionOperator op, Void context) {
            return false;
        }
    }
}
