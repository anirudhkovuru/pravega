/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.server.eventProcessor.requesthandlers;

import com.google.common.base.Preconditions;

import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.common.Exceptions;

import io.pravega.common.concurrent.Futures;
import io.pravega.controller.retryable.RetryableException;
import io.pravega.controller.store.stream.RGOperationContext;
import io.pravega.controller.store.stream.ReaderGroupState;
import io.pravega.controller.store.stream.StreamMetadataStore;

import io.pravega.controller.task.Stream.StreamMetadataTasks;
import io.pravega.controller.util.RetryHelper;
import io.pravega.shared.NameUtils;
import io.pravega.shared.controller.event.CreateReaderGroupEvent;
import lombok.extern.slf4j.Slf4j;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.Iterator;

/**
 * Request handler for executing a create operation for a KeyValueTable.
 */
@Slf4j
public class CreateReaderGroupTask implements ReaderGroupTask<CreateReaderGroupEvent> {

    private final StreamMetadataStore streamMetadataStore;
    private final StreamMetadataTasks streamMetadataTasks;
    private final ScheduledExecutorService executor;

    public CreateReaderGroupTask(final StreamMetadataTasks streamMetaTasks,
                                 final StreamMetadataStore streamMetaStore,
                                 final ScheduledExecutorService executor) {
        Preconditions.checkNotNull(streamMetaStore);
        Preconditions.checkNotNull(streamMetaTasks);
        Preconditions.checkNotNull(executor);
        this.streamMetadataStore = streamMetaStore;
        this.streamMetadataTasks = streamMetaTasks;
        this.executor = executor;
    }

    @Override
    public CompletableFuture<Void> execute(final CreateReaderGroupEvent request) {
        String scope = request.getScopeName();
        String readerGroup = request.getRgName();
        long requestId = request.getRequestId();

        final RGOperationContext context = streamMetadataStore.createRGContext(scope, readerGroup);
        log.info("Processing create request.");
        return RetryHelper.withRetriesAsync(() -> streamMetadataStore.getVersionedReaderGroupState(scope, readerGroup,
                true, context, executor)
                .thenCompose(state -> {
                    if (state.getObject().equals(ReaderGroupState.CREATING)) {
                        String scopedRGName = NameUtils.getScopedReaderGroupName(scope, readerGroup);
                        return streamMetadataStore.getReaderGroupConfigRecord(scope, readerGroup, context, executor)
                               .thenCompose(configRecord -> {
                               log.info("Adding metadata to Streams");
                               if (!ReaderGroupConfig.StreamDataRetention.values()[configRecord.getObject().getRetentionTypeOrdinal()]
                                   .equals(ReaderGroupConfig.StreamDataRetention.NONE)) {
                                   // update Stream metadata tables, if RG is a Subscriber
                                   Iterator<String> streamIter = configRecord.getObject().getStartingStreamCuts().keySet().iterator();
                                   return Futures.loop(() -> streamIter.hasNext(), () -> {
                                          Stream stream = Stream.of(streamIter.next());
                                          return streamMetadataStore.addSubscriber(stream.getScope(),
                                                       stream.getStreamName(), scopedRGName, null, executor);
                                      }, executor);
                                   }
                               return CompletableFuture.completedFuture(null);
                               }).thenCompose(v ->
                                  Futures.toVoid(streamMetadataStore.createStream(scope, NameUtils.getStreamForReaderGroup(readerGroup),
                                           StreamConfiguration.builder().scalingPolicy(ScalingPolicy.fixed(1)).build(), System.currentTimeMillis(), null, executor)
                                 .thenCompose(x -> streamMetadataStore.updateReaderGroupVersionedState(scope, readerGroup,
                                     ReaderGroupState.ACTIVE, state, context, executor))));
                    }
                    return null;
        }), e -> Exceptions.unwrap(e) instanceof RetryableException, Integer.MAX_VALUE, executor);
    }
}
