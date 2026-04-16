/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.queries;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.MockResolvedIndices;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.action.ResolvedIndices;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.InferenceFieldMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.search.vectors.KnnVectorQueryBuilder;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.RemoteClusterAware;
import org.elasticsearch.xpack.core.ml.search.SparseVectorQueryBuilder;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.sameInstance;

public class InferenceQueryUtilsTests extends ESTestCase {

    public void testHasLocalInferenceFieldReturnsTrueWhenResolvedIndicesNull() {
        QueryRewriteContext context = new QueryRewriteContext(
            null,
            null,
            null,
            TransportVersion.current(),
            RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY,
            null,
            null,
            null,
            null
        );
        assertTrue(InferenceQueryUtils.hasLocalInferenceField(context, "any_field"));
    }

    public void testHasLocalInferenceFieldReturnsFalseWhenNoInferenceFields() {
        QueryRewriteContext context = createContextWithInferenceFields(Map.of());
        assertFalse(InferenceQueryUtils.hasLocalInferenceField(context, "message"));
    }

    public void testHasLocalInferenceFieldReturnsFalseWhenFieldDoesNotMatch() {
        QueryRewriteContext context = createContextWithInferenceFields(Map.of("index-1", Map.of("semantic_field", "inference-id")));
        assertFalse(InferenceQueryUtils.hasLocalInferenceField(context, "message"));
    }

    public void testHasLocalInferenceFieldReturnsTrueWhenFieldMatches() {
        QueryRewriteContext context = createContextWithInferenceFields(Map.of("index-1", Map.of("semantic_field", "inference-id")));
        assertTrue(InferenceQueryUtils.hasLocalInferenceField(context, "semantic_field"));
    }

    public void testHasLocalInferenceFieldReturnsTrueWhenFieldMatchesInAnyIndex() {
        QueryRewriteContext context = createContextWithInferenceFields(
            Map.of("index-1", Map.of(), "index-2", Map.of("semantic_field", "inference-id"))
        );
        assertTrue(InferenceQueryUtils.hasLocalInferenceField(context, "semantic_field"));
    }

    public void testHasLocalInferenceFieldReturnsFalseWhenNoLocalIndices() {
        ResolvedIndices resolvedIndices = new MockResolvedIndices(
            Map.of("remote-cluster", new OriginalIndices(new String[] { "remote-index" }, IndicesOptions.DEFAULT)),
            new OriginalIndices(new String[0], IndicesOptions.DEFAULT),
            Map.of()
        );
        QueryRewriteContext context = new QueryRewriteContext(
            null,
            null,
            null,
            TransportVersion.current(),
            RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY,
            resolvedIndices,
            null,
            null,
            null
        );
        assertFalse(InferenceQueryUtils.hasLocalInferenceField(context, "any_field"));
    }

    public void testMatchInterceptorSkipsWrappingForNonInferenceField() throws IOException {
        QueryRewriteContext context = createContextWithInferenceFields(Map.of("index-1", Map.of("semantic_field", "inference-id")));
        MatchQueryBuilder matchQuery = new MatchQueryBuilder("message", "error");
        SemanticMatchQueryRewriteInterceptor interceptor = new SemanticMatchQueryRewriteInterceptor();

        QueryBuilder result = interceptor.interceptAndRewrite(context, matchQuery);
        assertThat(result, sameInstance(matchQuery));
    }

    public void testMatchInterceptorWrapsForInferenceField() throws IOException {
        QueryRewriteContext context = createContextWithInferenceFields(Map.of("index-1", Map.of("semantic_field", "inference-id")));
        MatchQueryBuilder matchQuery = new MatchQueryBuilder("semantic_field", "test query");
        SemanticMatchQueryRewriteInterceptor interceptor = new SemanticMatchQueryRewriteInterceptor();

        QueryBuilder result = interceptor.interceptAndRewrite(context, matchQuery);
        assertThat(result, instanceOf(InterceptedInferenceMatchQueryBuilder.class));
    }

    public void testSparseVectorInterceptorSkipsWrappingForNonInferenceField() throws IOException {
        QueryRewriteContext context = createContextWithInferenceFields(Map.of("index-1", Map.of("semantic_field", "inference-id")));
        SparseVectorQueryBuilder sparseQuery = new SparseVectorQueryBuilder("regular_field", List.of(), null, null, null, null);
        SemanticSparseVectorQueryRewriteInterceptor interceptor = new SemanticSparseVectorQueryRewriteInterceptor();

        QueryBuilder result = interceptor.interceptAndRewrite(context, sparseQuery);
        assertThat(result, sameInstance(sparseQuery));
    }

    public void testKnnInterceptorSkipsWrappingForNonInferenceField() throws IOException {
        QueryRewriteContext context = createContextWithInferenceFields(Map.of("index-1", Map.of("semantic_field", "inference-id")));
        KnnVectorQueryBuilder knnQuery = new KnnVectorQueryBuilder("regular_field", new float[] { 1.0f }, 10, 100, null, null, null);
        SemanticKnnVectorQueryRewriteInterceptor interceptor = new SemanticKnnVectorQueryRewriteInterceptor();

        QueryBuilder result = interceptor.interceptAndRewrite(context, knnQuery);
        assertThat(result, sameInstance(knnQuery));
    }

    private QueryRewriteContext createContextWithInferenceFields(Map<String, Map<String, String>> indexInferenceFields) {
        Map<Index, IndexMetadata> indexMetadata = new HashMap<>();
        for (var indexEntry : indexInferenceFields.entrySet()) {
            String indexName = indexEntry.getKey();
            Index index = new Index(indexName, randomAlphaOfLength(10));
            IndexMetadata.Builder builder = IndexMetadata.builder(index.getName())
                .settings(
                    Settings.builder()
                        .put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current())
                        .put(IndexMetadata.SETTING_INDEX_UUID, index.getUUID())
                )
                .numberOfShards(1)
                .numberOfReplicas(0);

            for (var fieldEntry : indexEntry.getValue().entrySet()) {
                builder.putInferenceField(
                    new InferenceFieldMetadata(fieldEntry.getKey(), fieldEntry.getValue(), new String[] { fieldEntry.getKey() }, null)
                );
            }

            indexMetadata.put(index, builder.build());
        }

        ResolvedIndices resolvedIndices = new MockResolvedIndices(
            Map.of(),
            new OriginalIndices(indexInferenceFields.keySet().toArray(new String[0]), IndicesOptions.DEFAULT),
            indexMetadata
        );

        return new QueryRewriteContext(
            null,
            null,
            null,
            TransportVersion.current(),
            RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY,
            resolvedIndices,
            null,
            null,
            null
        );
    }
}
