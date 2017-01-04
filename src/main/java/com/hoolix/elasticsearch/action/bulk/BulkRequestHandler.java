package com.hoolix.elasticsearch.action.bulk;

import akka.kafka.ConsumerMessage;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.logging.log4j.util.Supplier;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.bulk.Retry;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;

import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

/**
 * Hoolix 2017
 * Created by simon on 1/4/17.
 *
 * Abstracts the low-level details of bulk request handling
 */
abstract class BulkRequestHandler {
    protected final Logger logger;
    protected final Client client;

    protected BulkRequestHandler(Client client) {
        this.client = client;
        this.logger = Loggers.getLogger(getClass(), client.settings());
    }


    public abstract void execute(BulkRequest bulkRequest, ConsumerMessage.CommittableOffsetBatch offsetBatch, long executionId);

    public abstract boolean awaitClose(long timeout, TimeUnit unit) throws InterruptedException;


    public static BulkRequestHandler syncHandler(Client client, BackoffPolicy backoffPolicy, BulkProcessor.Listener listener) {
        return new SyncBulkRequestHandler(client, backoffPolicy, listener);
    }

    public static BulkRequestHandler asyncHandler(Client client, BackoffPolicy backoffPolicy, BulkProcessor.Listener listener, int concurrentRequests) {
        return new AsyncBulkRequestHandler(client, backoffPolicy, listener, concurrentRequests);
    }

    private static class SyncBulkRequestHandler extends BulkRequestHandler {
        private final BulkProcessor.Listener listener;
        private final BackoffPolicy backoffPolicy;

        public SyncBulkRequestHandler(Client client, BackoffPolicy backoffPolicy, BulkProcessor.Listener listener) {
            super(client);
            this.backoffPolicy = backoffPolicy;
            this.listener = listener;
        }

        @Override
        public void execute(BulkRequest bulkRequest, ConsumerMessage.CommittableOffsetBatch offsetBatch, long executionId) {
            boolean afterCalled = false;
            try {
                listener.beforeBulk(executionId, bulkRequest);
                BulkResponse bulkResponse = Retry
                        .on(EsRejectedExecutionException.class)
                        .policy(backoffPolicy)
                        .withSyncBackoff(client, bulkRequest);
                afterCalled = true;
                listener.afterBulk(executionId, bulkRequest, bulkResponse, offsetBatch);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                logger.info((Supplier<?>) () -> new ParameterizedMessage("Bulk request {} has been cancelled.", executionId), e);
                if (!afterCalled) {
                    listener.afterBulk(executionId, bulkRequest, e);
                }
            } catch (Exception e) {
                logger.warn((Supplier<?>) () -> new ParameterizedMessage("Failed to execute bulk request {}.", executionId), e);
                if (!afterCalled) {
                    listener.afterBulk(executionId, bulkRequest, e);
                }
            }
        }

        @Override
        public boolean awaitClose(long timeout, TimeUnit unit) throws InterruptedException {
            // we are "closed" immediately as there is no request in flight
            return true;
        }
    }

    private static class AsyncBulkRequestHandler extends BulkRequestHandler {
        private final BackoffPolicy backoffPolicy;
        private final BulkProcessor.Listener listener;
        private final Semaphore semaphore;
        private final int concurrentRequests;

        private AsyncBulkRequestHandler(Client client, BackoffPolicy backoffPolicy, BulkProcessor.Listener listener, int concurrentRequests) {
            super(client);
            this.backoffPolicy = backoffPolicy;
            assert concurrentRequests > 0;
            this.listener = listener;
            this.concurrentRequests = concurrentRequests;
            this.semaphore = new Semaphore(concurrentRequests);
        }

        @Override
        public void execute(BulkRequest bulkRequest, ConsumerMessage.CommittableOffsetBatch offsetBatch, long executionId) {
            boolean bulkRequestSetupSuccessful = false;
            boolean acquired = false;
            try {
                listener.beforeBulk(executionId, bulkRequest);
                semaphore.acquire();
                acquired = true;
                Retry.on(EsRejectedExecutionException.class)
                        .policy(backoffPolicy)
                        .withAsyncBackoff(client, bulkRequest, new ActionListener<BulkResponse>() {
                            @Override
                            public void onResponse(BulkResponse response) {
                                try {
                                    listener.afterBulk(executionId, bulkRequest, response, offsetBatch);
                                } finally {
                                    semaphore.release();
                                }
                            }

                            @Override
                            public void onFailure(Exception e) {
                                try {
                                    listener.afterBulk(executionId, bulkRequest, e);
                                } finally {
                                    semaphore.release();
                                }
                            }
                        });
                bulkRequestSetupSuccessful = true;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                logger.info((Supplier<?>) () -> new ParameterizedMessage("Bulk request {} has been cancelled.", executionId), e);
                listener.afterBulk(executionId, bulkRequest, e);
            } catch (Exception e) {
                logger.warn((Supplier<?>) () -> new ParameterizedMessage("Failed to execute bulk request {}.", executionId), e);
                listener.afterBulk(executionId, bulkRequest, e);
            } finally {
                if (!bulkRequestSetupSuccessful && acquired) {  // if we fail on client.bulk() release the semaphore
                    semaphore.release();
                }
            }
        }

        @Override
        public boolean awaitClose(long timeout, TimeUnit unit) throws InterruptedException {
            if (semaphore.tryAcquire(this.concurrentRequests, timeout, unit)) {
                semaphore.release(this.concurrentRequests);
                return true;
            }
            return false;
        }
    }
}

