/*
 * Copyright (c) 2018, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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
package org.wso2.siddhi.core.event.stream;

import org.wso2.siddhi.core.exception.SiddhiAppRuntimeException;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.ObjectStreamClass;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Implementation class for Deep {@link StreamEvent} cloner to be used when creating
 * {@link org.wso2.siddhi.core.partition.PartitionRuntime}
 */
public class StreamEventDeepCloner implements StreamEventCloner {

    protected final int beforeWindowDataSize;
    protected final int onAfterWindowDataSize;
    protected final int outputDataSize;
    protected final StreamEventPool streamEventPool;

    public StreamEventDeepCloner(MetaStreamEvent metaStreamEvent, StreamEventPool streamEventPool) {
        this.streamEventPool = streamEventPool;
        this.beforeWindowDataSize = metaStreamEvent.getBeforeWindowData().size();
        this.onAfterWindowDataSize = metaStreamEvent.getOnAfterWindowData().size();
        this.outputDataSize = metaStreamEvent.getOutputData().size();

    }

    /**
     * Method to copy new StreamEvent from StreamEvent
     *
     * @param streamEvent StreamEvent to be copied
     * @return StreamEvent
     */
    public StreamEvent copyStreamEvent(StreamEvent streamEvent) {
        StreamEvent borrowedEvent = streamEventPool.borrowEvent();
        if (beforeWindowDataSize > 0) {
            borrowedEvent.setBeforeWindowData(clone(streamEvent.getBeforeWindowData()));
        }
        if (onAfterWindowDataSize > 0) {
            borrowedEvent.setOnAfterWindowData(clone(streamEvent.getOnAfterWindowData()));
        }
        if (outputDataSize > 0) {
            borrowedEvent.setOutputData(clone(streamEvent.getOutputData()));
        }
        borrowedEvent.setType(streamEvent.getType());
        borrowedEvent.setTimestamp(streamEvent.getTimestamp());
        return borrowedEvent;
    }

    private static <T extends Serializable> T clone(final T object) {
        if (object == null) {
            return null;
        }
        final byte[] objectData = serialize(object);
        final ByteArrayInputStream bais = new ByteArrayInputStream(objectData);

        try (ClassLoaderAwareObjectInputStream in = new ClassLoaderAwareObjectInputStream(bais,
                object.getClass().getClassLoader())) {
            /*
             * when we serialize and deserialize an object,
             * it is reasonable to assume the deserialized object
             * is of the same type as the original serialized object
             */
            @SuppressWarnings("unchecked") // see above
            final T readObject = (T) in.readObject();
            return readObject;
        } catch (final ClassNotFoundException ex) {
            throw new SiddhiAppRuntimeException("ClassNotFoundException while reading cloned object data", ex);
        } catch (final IOException ex) {
            throw new SiddhiAppRuntimeException("IOException while reading or closing cloned object data", ex);
        }
    }

    private static byte[] serialize(final Serializable obj) {
        final ByteArrayOutputStream baos = new ByteArrayOutputStream(512);
        serialize(obj, baos);
        return baos.toByteArray();
    }

    private static void serialize(final Serializable obj, final OutputStream outputStream) {
        if (outputStream != null) {
            try (ObjectOutputStream out = new ObjectOutputStream(outputStream)) {
                out.writeObject(obj);
            } catch (final IOException ex) {
                throw new SiddhiAppRuntimeException(ex);

            }
        }
    }

    static class ClassLoaderAwareObjectInputStream extends ObjectInputStream {
        private static final Map<String, Class<?>> primitiveTypes =
                new HashMap<>();
        static {
            primitiveTypes.put("byte", byte.class);
            primitiveTypes.put("short", short.class);
            primitiveTypes.put("int", int.class);
            primitiveTypes.put("long", long.class);
            primitiveTypes.put("float", float.class);
            primitiveTypes.put("double", double.class);
            primitiveTypes.put("boolean", boolean.class);
            primitiveTypes.put("char", char.class);
            primitiveTypes.put("void", void.class);
        }

        private final ClassLoader classLoader;

        /**
         * Constructor.
         *
         * @param in          The <code>InputStream</code>.
         * @param classLoader classloader to use
         * @throws IOException if an I/O error occurs while reading stream header.
         * @see java.io.ObjectInputStream
         */
        ClassLoaderAwareObjectInputStream(final InputStream in, final ClassLoader classLoader) throws IOException {
            super(in);
            this.classLoader = classLoader;
        }

        /**
         * Overridden version that uses the parameterized <code>ClassLoader</code> or the <code>ClassLoader</code>
         * of the current <code>Thread</code> to resolve the class.
         *
         * @param desc An instance of class <code>ObjectStreamClass</code>.
         * @return A <code>Class</code> object corresponding to <code>desc</code>.
         * @throws IOException            Any of the usual Input/Output exceptions.
         * @throws ClassNotFoundException If class of a serialized object cannot be found.
         */
        @Override
        protected Class<?> resolveClass(final ObjectStreamClass desc) throws IOException, ClassNotFoundException {
            final String name = desc.getName();
            try {
                return Class.forName(name, false, classLoader);
            } catch (final ClassNotFoundException ex) {
                try {
                    return Class.forName(name, false, Thread.currentThread().getContextClassLoader());
                } catch (final ClassNotFoundException cnfe) {
                    final Class<?> cls = primitiveTypes.get(name);
                    if (cls != null) {
                        return cls;
                    }
                    throw cnfe;
                }
            }
        }
    }
}
