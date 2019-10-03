/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.orc;

import com.facebook.presto.orc.TupleDomainFilter.BigintRange;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.Subfield;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.spi.type.TypeSignature;
import com.facebook.presto.spi.type.TypeSignatureParameter;
import com.facebook.presto.type.TypeRegistry;
import com.google.common.collect.ContiguousSet;
import com.google.common.collect.DiscreteDomain;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Range;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;

import java.io.File;
import java.io.IOException;
import java.math.BigInteger; //import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.orc.OrcReader.MAX_BATCH_SIZE;
import static com.facebook.presto.orc.OrcTester.Format.DWRF;
import static com.facebook.presto.orc.OrcTester.quickSelectiveOrcTester;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.SmallintType.SMALLINT;
import static com.facebook.presto.spi.type.TinyintType.TINYINT;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR; //import static com.facebook.presto.testing.TestingConnectorSession.SESSION;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.testng.Assert.assertEquals;

@SuppressWarnings("MethodMayBeStatic")
@State(Scope.Thread)
@OutputTimeUnit(MILLISECONDS)
@Fork(1)
@Warmup(iterations = 0, time = 1000, timeUnit = MILLISECONDS)
@Measurement(iterations = 10, time = 1000, timeUnit = MILLISECONDS)
@BenchmarkMode(Mode.AverageTime)
public class BenchmarkSelectiveOrcReader
{
    private final OrcTester tester = quickSelectiveOrcTester();

    private static ContiguousSet<Integer> intsBetween(int lowerInclusive, int upperExclusive)
    {
        return ContiguousSet.create(Range.closedOpen(lowerInclusive, upperExclusive), DiscreteDomain.integers());
    }

    @State(Scope.Thread)
    public static class SelectiveOrcReaderState
    {
        @Setup(Level.Trial)
        public void trialSetup()
        {
//            orcFile = new File("/data/users/ismailov/readers_test/part-00000");
            orcFile = new File("/data/users/ismailov/bi/dim_all_users_fast/2019-09-19/part-00000");
            totalSum = BigInteger.ZERO;
            schema = ImmutableList.of(BIGINT, VARCHAR, BIGINT, VARCHAR, SMALLINT, VARCHAR, VARCHAR, INTEGER, SMALLINT, VARCHAR, VARCHAR, TINYINT, TINYINT, TINYINT, TINYINT, TINYINT, TINYINT, TINYINT, TINYINT, TINYINT, TINYINT, TINYINT, TINYINT, TINYINT, VARCHAR);
            //schema = ImmutableList.of(BIGINT);
            TypeManager typeManager = new TypeRegistry();
            Type arrayOfVarchar = typeManager.getType(new TypeSignature(
                    StandardTypes.ARRAY,
                    ImmutableList.of(TypeSignatureParameter.of(VARCHAR.getTypeSignature()))));
            Type arrayOfDouble = typeManager.getType(new TypeSignature(
                    StandardTypes.ARRAY,
                    ImmutableList.of(TypeSignatureParameter.of(DOUBLE.getTypeSignature()))));
//            schema = ImmutableList.of(BIGINT, VARCHAR, DOUBLE, VARCHAR, arrayOfVarchar, arrayOfDouble);
//            schema = ImmutableList.of(BIGINT);
        }
        @TearDown(Level.Trial)
        public void trielTearDown()
        {
            try {
                TimeUnit.MINUTES.sleep(5);
            }
            catch (java.lang.InterruptedException ex) {
            }
        }
        public File orcFile;
        public BigInteger totalSum;
        public List<Type> schema;
    }

    @Benchmark
    public void testBenchmarkBigIntRead(SelectiveOrcReaderState state, Blackhole blackhole)
            throws Exception
    {
//        assertEquals(DateTimeZone.getDefault(), HIVE_STORAGE_TIME_ZONE);
        state.totalSum = BigInteger.ZERO;
        testRoundTripNumeric(intsBetween(0, 31_234), BigintRange.of(Long.MIN_VALUE, Long.MAX_VALUE - 1, false), state, blackhole);
//        System.out.println("CHECKSUM: " + state.totalSum);
    }

    private void testRoundTripNumeric(Iterable<? extends Number> values, TupleDomainFilter filter, SelectiveOrcReaderState state, Blackhole blackhole)
            throws Exception
    {
        readFileContentsPresto(DWRF.getOrcEncoding(), OrcPredicate.TRUE, Optional.of(ImmutableMap.of()), ImmutableList.of(), ImmutableMap.of(), ImmutableMap.of(), state, blackhole);
    }

    public void readFileContentsPresto(
            OrcEncoding orcEncoding,
            OrcPredicate orcPredicate,
            Optional<Map<Integer, Map<Subfield, TupleDomainFilter>>> filters,
            List<FilterFunction> filterFunctions,
            Map<Integer, Integer> filterFunctionInputMapping,
            Map<Integer, List<Subfield>> requiredSubfields,
            SelectiveOrcReaderState state,
            Blackhole blackhole)
            throws IOException
    {
        try (OrcSelectiveRecordReader recordReader = tester.createCustomOrcSelectiveRecordReader(state.orcFile, orcEncoding, orcPredicate, state.schema, MAX_BATCH_SIZE, filters.orElse(ImmutableMap.of()), filterFunctions, filterFunctionInputMapping, requiredSubfields)) {
            assertEquals(recordReader.getReaderPosition(), 0);
            assertEquals(recordReader.getFilePosition(), 0);

            int rowsProcessed = 0;
            while (true) {
                Page page = recordReader.getNextPage();
                if (page == null) {
                    break;
                }
                int positionCount = page.getPositionCount();
                if (positionCount == 0) {
                    continue;
                }
//                assertTrue(expectedValues.get(0).size() >= rowsProcessed + positionCount);
                for (int i = 0; i < state.schema.size(); i++) {
                    Type type = state.schema.get(i);
                    Block block = page.getBlock(i);
                    blackhole.consume(block);
/*
                    List<Object> data = new ArrayList<>(positionCount);
                    for (int position = 0; position < positionCount; position++) {
                        data.add(type.getObjectValue(SESSION, block, position));
                        if (i == 0) {
                            state.totalSum = state.totalSum.add(BigInteger.valueOf(type.getLong(block, position)));
                        }
                        else if (i == 1) {
                            state.totalSum = state.totalSum.add(BigInteger.valueOf(type.getSlice(block, position).length()));
                        }
                        else if (i == 2) {
                            state.totalSum = state.totalSum.add(BigInteger.valueOf(Double.doubleToLongBits(type.getDouble(block, position))));
                        }
                        else if (i == 3) {
                            state.totalSum = state.totalSum.add(BigInteger.valueOf(type.getSlice(block, position).length()));
                        }
                        else if (i == 4) {
                            state.totalSum = state.totalSum.add(BigInteger.valueOf(((List) type.getObjectValue(SESSION, block, position)).size()));
                        }
                        else if (i == 5) {
                            state.totalSum = state.totalSum.add(BigInteger.valueOf(((List) type.getObjectValue(SESSION, block, position)).size()));
                        }
                    }
*/
                }
                rowsProcessed += positionCount;
            }
//            assertEquals(rowsProcessed, expectedValues.get(0).size());
        }
    }

    public static void main(String[] args)
            throws Throwable
    {
        Options options = new OptionsBuilder()
                .verbosity(VerboseMode.NORMAL)
                .include(".*" + BenchmarkSelectiveOrcReader.class.getSimpleName() + ".*")
                .build();

        new Runner(options).run();
    }
}
