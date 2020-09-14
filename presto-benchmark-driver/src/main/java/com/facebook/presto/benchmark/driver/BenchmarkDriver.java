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
package com.facebook.presto.benchmark.driver;

import com.facebook.presto.client.ClientSession;
import com.google.common.collect.ImmutableList;
import com.google.common.net.HostAndPort;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static java.util.Objects.requireNonNull;

public class BenchmarkDriver
        implements Closeable
{
    private final ClientSession clientSession;
    private final List<BenchmarkQuery> queries;
    private final BenchmarkResultsStore resultsStore;
    private final BenchmarkQueryRunner queryRunner;
    private final int threads;

    public BenchmarkDriver(BenchmarkResultsStore resultsStore,
                           ClientSession clientSession,
                           Iterable<BenchmarkQuery> queries,
                           int warm,
                           int runs,
                           boolean debug,
                           int maxFailures,
                           Optional<HostAndPort> socksProxy,
                           int threads)
    {
        this.resultsStore = requireNonNull(resultsStore, "resultsStore is null");
        this.clientSession = requireNonNull(clientSession, "clientSession is null");
        this.queries = ImmutableList.copyOf(requireNonNull(queries, "queries is null"));
        this.threads = threads;

        queryRunner = new BenchmarkQueryRunner(warm, runs, debug, maxFailures, clientSession.getServer(), socksProxy);
    }

    public void run(Suite suite) throws ExecutionException, InterruptedException
    {
        // select queries to run
        List<BenchmarkQuery> queries = suite.selectQueries(this.queries);
        if (queries.isEmpty()) {
            return;
        }

        Map<String, String> properties = new HashMap<>();
        properties.putAll(clientSession.getProperties());
        properties.putAll(suite.getSessionProperties());
        ClientSession session = ClientSession.builder(clientSession)
                .withProperties(properties)
                .build();

        // select schemas to use
        List<BenchmarkSchema> benchmarkSchemas;
        if (!suite.getSchemaNameTemplates().isEmpty()) {
            List<String> schemas = queryRunner.getSchemas(session);
            benchmarkSchemas = suite.selectSchemas(schemas);
        }
        else {
            benchmarkSchemas = ImmutableList.of(new BenchmarkSchema(session.getSchema()));
        }
        if (benchmarkSchemas.isEmpty()) {
            return;
        }

        if (threads == 1) {
            for (BenchmarkSchema benchmarkSchema : benchmarkSchemas) {
                ClientSession schemaSession = ClientSession.builder(session)
                        .withCatalog(session.getCatalog())
                        .withSchema(benchmarkSchema.getName())
                        .build();
                for (BenchmarkQuery benchmarkQuery : queries) {
                    BenchmarkQueryResult result = queryRunner.execute(suite, schemaSession, benchmarkQuery);
                    resultsStore.store(benchmarkSchema, result);
                }
            }
        }
        else {
            for (BenchmarkSchema benchmarkSchema : benchmarkSchemas) {
                ClientSession schemaSession = ClientSession.builder(session)
                        .withCatalog(session.getCatalog())
                        .withSchema(benchmarkSchema.getName())
                        .build();
                ExecutorService executorService = Executors.newFixedThreadPool(threads);
                List<Future<BenchmarkQueryResult>> results = new ArrayList<>();
                for (BenchmarkQuery benchmarkQuery : queries) {
                    results.add(executorService.submit(() -> queryRunner.execute(suite, schemaSession, benchmarkQuery)));
                }
                for (Future<BenchmarkQueryResult> result : results) {
                    resultsStore.store(benchmarkSchema, result.get());
                }
                executorService.shutdown();
            }
        }
    }

    @Override
    public void close()
    {
        queryRunner.close();
    }
}
