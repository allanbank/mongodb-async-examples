/*
 *           Copyright 2013 - Allanbank Consulting, Inc.
 * 
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
package async;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.allanbank.mongodb.Callback;

/**
 * AbstractRetryCallbackHandler provides a base class for {@link Callback}
 * instances that will retry the operation.
 * 
 * @param <V>
 *            The type of the callback's result.
 * 
 * @copyright 2013, Allanbank Consulting, Inc., All Rights Reserved
 */
public abstract class AbstractRetryCallbackHandler<V> implements Callback<V> {

    /** The maximum amount of time to pause between retries. */
    public static final long MAX_PAUSE_MS = TimeUnit.MINUTES.toMillis(1);

    /** The executor for retries. */
    private final ScheduledExecutorService myExecutor;

    /** The maximum number of times to retry. */
    private final int myMaximumRetries;

    /** The number of milliseconds to wait before attempting another retry. */
    private long myNextPauseMS;

    /** The number of times we have attemtped to retry. */
    private int myRetries;

    /**
     * Creates a new AbstractRetryCallbackHandler.
     * 
     * @param retries
     *            The number of times we have attempted to retry.
     * @param executor
     *            The executor for retries.
     */
    public AbstractRetryCallbackHandler(final int retries,
            final ScheduledExecutorService executor) {
        myMaximumRetries = retries;
        myExecutor = executor;
        myRetries = 0;
        myNextPauseMS = 1;
    }

    /**
     * Called when all of the retry attempts have been exhausted.
     * 
     * @param thrown
     *            The error causing the retry.
     */
    public abstract void dead(final Throwable thrown);

    /**
     * {@inheritDoc}
     * <p>
     * Overridden to retry the request with a suitable pause between the
     * retries.
     * </p>
     */
    @Override
    public void exception(final Throwable thrown) {
        myRetries += 1;
        if (myRetries < myMaximumRetries) {

            myExecutor.schedule(new Runnable() {

                @Override
                public void run() {
                    retry(thrown);
                }
            }, myNextPauseMS, TimeUnit.MILLISECONDS);

            myNextPauseMS = Math.min(myNextPauseMS * 2, MAX_PAUSE_MS);
        }
        else {
            dead(thrown);
        }
    }

    /**
     * Called to attempt a retry.
     * 
     * @param thrown
     *            The error causing the retry.
     */
    public abstract void retry(final Throwable thrown);
}
