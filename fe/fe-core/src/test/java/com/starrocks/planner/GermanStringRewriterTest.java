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

import com.starrocks.catalog.BenchmarkTable;
import com.starrocks.catalog.Column;
import com.starrocks.connector.benchmark.BenchmarkCatalogConfig;
import com.starrocks.connector.benchmark.BenchmarkConfig;
import com.starrocks.qe.SessionVariable;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.SlotRef;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.thrift.TPrimitiveType;
import com.starrocks.thrift.TSlotDescriptor;
import com.starrocks.type.IntegerType;
import com.starrocks.type.PrimitiveType;
import com.starrocks.type.TypeFactory;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Direct unit tests for {@link GermanStringRewriter}, exercising slot/expr
 * rewrites without a full planner pipeline.
 */
public class GermanStringRewriterTest {

    @Test
    public void testShouldApplyReadsSessionVariable() {
        SessionVariable sv = new SessionVariable();
        assertFalse(GermanStringRewriter.shouldApply(sv));
        sv.setEnableGermanString(true);
        assertTrue(GermanStringRewriter.shouldApply(sv));
        assertFalse(GermanStringRewriter.shouldApply(null));
    }

    @Test
    public void testRewriteFlipsVarcharSlotToGermanString() {
        ExecPlan execPlan = new ExecPlan();
        DescriptorTable descTbl = execPlan.getDescTbl();

        TupleDescriptor tuple = descTbl.createTupleDescriptor();
        tuple.setIsMaterialized(true);
        SlotDescriptor varcharSlot = descTbl.addSlotDescriptor(tuple);
        varcharSlot.setType(TypeFactory.createVarcharType(64));
        varcharSlot.setOriginType(TypeFactory.createVarcharType(64));
        varcharSlot.setIsMaterialized(true);

        SlotDescriptor intSlot = descTbl.addSlotDescriptor(tuple);
        intSlot.setType(IntegerType.INT);
        intSlot.setIsMaterialized(true);

        GermanStringRewriter.rewrite(execPlan);

        // VARCHAR slot becomes GERMAN_STRING, length preserved.
        assertEquals(PrimitiveType.GERMAN_STRING, varcharSlot.getType().getPrimitiveType());
        assertEquals(PrimitiveType.GERMAN_STRING, varcharSlot.getOriginType().getPrimitiveType());
        // INT slot untouched.
        assertEquals(PrimitiveType.INT, intSlot.getType().getPrimitiveType());

        // Thrift serialization reports GERMAN_STRING.
        TSlotDescriptor tslot = varcharSlot.toThrift();
        assertEquals(TPrimitiveType.GERMAN_STRING,
                tslot.getSlotType().getTypes().get(0).getScalar_type().getType());
    }

    @Test
    public void testRewriteSkipsConnectorExternalTable() {
        ExecPlan execPlan = new ExecPlan();
        DescriptorTable descTbl = execPlan.getDescTbl();

        // External/connector table tuple — should be left alone.
        TupleDescriptor externalTuple = descTbl.createTupleDescriptor();
        externalTuple.setTable(newBenchmarkTable());
        externalTuple.setIsMaterialized(true);
        SlotDescriptor externalSlot = descTbl.addSlotDescriptor(externalTuple);
        externalSlot.setType(TypeFactory.createVarcharType(64));
        externalSlot.setOriginType(TypeFactory.createVarcharType(64));
        externalSlot.setIsMaterialized(true);

        GermanStringRewriter.rewrite(execPlan);

        assertEquals(PrimitiveType.VARCHAR, externalSlot.getType().getPrimitiveType());
        assertEquals(PrimitiveType.VARCHAR, externalSlot.getOriginType().getPrimitiveType());
    }

    @Test
    public void testRewriteFragmentOutputExprsDoesNotTouchExecPlanOutputExprs() {
        // Simulate the StmtExecutor invariant: ExecPlan.outputExprs is the
        // authoritative source for MySQL wire metadata (getOriginType). The
        // root fragment carries a *cloned* list that flows to BE. Rewriting
        // the fragment clone must not mutate the ExecPlan list.
        ExecPlan execPlan = new ExecPlan();
        DescriptorTable descTbl = execPlan.getDescTbl();

        TupleDescriptor tuple = descTbl.createTupleDescriptor();
        tuple.setIsMaterialized(true);
        SlotDescriptor slot = descTbl.addSlotDescriptor(tuple);
        slot.setLabel("s_name");
        slot.setType(TypeFactory.createVarcharType(25));
        slot.setOriginType(TypeFactory.createVarcharType(25));
        slot.setIsMaterialized(true);

        SlotRef execPlanRef = new SlotRef(slot);
        // Capture the pre-rewrite wire metadata type seen via getOriginType().
        assertEquals(PrimitiveType.VARCHAR, execPlanRef.getOriginType().getPrimitiveType());

        // Build a fragment whose outputExprs carries a clone of execPlanRef.
        PlanFragmentId fid = new PlanFragmentId(0);
        PlanFragment fragment = new PlanFragment(fid, null, DataPartition.UNPARTITIONED);
        fragment.setOutputExprs(java.util.Arrays.asList((Expr) execPlanRef));
        execPlan.getFragments().add(fragment);

        GermanStringRewriter.rewrite(execPlan);

        // Fragment's clone has GERMAN_STRING on the BE-facing type.
        Expr clonedOutputExpr = fragment.getOutputExprs().get(0);
        assertEquals(PrimitiveType.GERMAN_STRING, clonedOutputExpr.getType().getPrimitiveType());

        // The execPlan-owned SlotRef's wire metadata (getOriginType) must stay
        // VARCHAR so the JDBC client sees the type it expects. Note: because
        // Expr.originType is captured per-instance at construction time, it is
        // independent from the slot descriptor's originType we flipped.
        assertEquals(PrimitiveType.VARCHAR, execPlanRef.getOriginType().getPrimitiveType());

        // The shared descriptor, meanwhile, is GERMAN_STRING (this is what BE
        // sees via SlotDescriptor.toThrift()).
        assertEquals(PrimitiveType.GERMAN_STRING, slot.getType().getPrimitiveType());
    }

    @Test
    public void testRewritePreservesSlotLength() {
        ExecPlan execPlan = new ExecPlan();
        DescriptorTable descTbl = execPlan.getDescTbl();
        TupleDescriptor tuple = descTbl.createTupleDescriptor();
        tuple.setIsMaterialized(true);
        SlotDescriptor slot = descTbl.addSlotDescriptor(tuple);
        slot.setType(TypeFactory.createVarcharType(123));
        slot.setOriginType(TypeFactory.createVarcharType(123));
        slot.setIsMaterialized(true);

        GermanStringRewriter.rewrite(execPlan);

        assertEquals(123, ((com.starrocks.type.ScalarType) slot.getType()).getLength());
        assertEquals(123, ((com.starrocks.type.ScalarType) slot.getOriginType()).getLength());
    }

    @Test
    public void testRewriteIsNoOpOnEmptyPlan() {
        ExecPlan execPlan = new ExecPlan();
        // Does not throw, does not mutate anything.
        GermanStringRewriter.rewrite(execPlan);
        GermanStringRewriter.rewrite(null);
    }

    @Test
    public void testRewriteFlipsSlotRefTypeThroughDescriptor() {
        ExecPlan execPlan = new ExecPlan();
        DescriptorTable descTbl = execPlan.getDescTbl();
        TupleDescriptor tuple = descTbl.createTupleDescriptor();
        tuple.setIsMaterialized(true);
        SlotDescriptor slot = descTbl.addSlotDescriptor(tuple);
        slot.setLabel("col");
        slot.setType(TypeFactory.createVarcharType(16));
        slot.setOriginType(TypeFactory.createVarcharType(16));
        slot.setIsMaterialized(true);

        SlotRef slotRef = new SlotRef(slot);
        // Attach via a fragment so the rewriter walks it.
        PlanFragment fragment = new PlanFragment(new PlanFragmentId(0), null, DataPartition.UNPARTITIONED);
        fragment.setOutputExprs(java.util.Arrays.asList((Expr) slotRef));
        execPlan.getFragments().add(fragment);

        assertEquals(PrimitiveType.VARCHAR, fragment.getOutputExprs().get(0).getType().getPrimitiveType());

        GermanStringRewriter.rewrite(execPlan);

        // After rewrite, the fragment's cloned SlotRef type is GERMAN_STRING.
        assertEquals(PrimitiveType.GERMAN_STRING,
                fragment.getOutputExprs().get(0).getType().getPrimitiveType());
    }

    // Helpers ----------------------------------------------------------------

    private static BenchmarkTable newBenchmarkTable() {
        List<Column> schema = List.of(new Column("c1", IntegerType.INT));
        BenchmarkConfig config = new BenchmarkConfig();
        Map<String, String> properties = new HashMap<>();
        properties.put(BenchmarkConfig.SCALE, "1.0");
        config.loadConfig(properties);
        BenchmarkCatalogConfig catalogConfig = BenchmarkCatalogConfig.from(config);
        return new BenchmarkTable(1L, "benchmark", "db", "t", schema, catalogConfig);
    }
}
