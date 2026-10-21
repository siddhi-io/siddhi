/*
 * Copyright (c) 2016, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.siddhi.core.util;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Thread Barrier used to avoid concurrency issues during event processing
 */
public class ThreadBarrier {

    private ReentrantLock lock = new ReentrantLock();
    private AtomicInteger counter = new AtomicInteger();
    private ThreadLocal<MutableInteger> localEnters = ThreadLocal.withInitial(() -> new MutableInteger(0));

    public void enter() {
        if (localEnters.get().incrementAndGet() > 1) {
            return;
        }
        if (lock.isLocked()) {
            lock.lock();
            lock.unlock();
        }
        counter.incrementAndGet();
    }

    public void exit() {
        if (localEnters.get().decrementAndGet() == 0) {
            counter.decrementAndGet();
        }
    }

    public int getActiveThreads() {
        return counter.get();
    }

    public void lock() {
        lock.lock();
    }

    public void unlock() {
        lock.unlock();
    }

    /**
     * Simple non-thread-safe integer wrapper to be used in ThreadLocal
     */
    private static class MutableInteger {

        private int value;

        public MutableInteger(int initialValue) {
            this.value = initialValue;
        }

        public int incrementAndGet() {
            return ++value;
        }

        public int decrementAndGet() {
            return --value;
        }
    }

}
