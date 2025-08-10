package org.wso2.siddhi.core.util.kvstore;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.concurrent.ConcurrentHashMap;
import java.lang.reflect.Constructor;

/**
 * Manages the creation of Key-Value store client instances based on system configuration.
 */
public class KeyValueStoreManager {

    private static final Logger log = LoggerFactory.getLogger(KeyValueStoreManager.class);

    public static final String KEYVALUE_STORE_TYPE_PROPERTY = "siddhi.kvstore.type";
    public static final String REDIS_TYPE = "redis";
    public static final String VALKEY_TYPE = "valkey";
    public static final String DEFAULT_KV_STORE_TYPE = REDIS_TYPE;
    public static final String KEYVALUE_STORE_CLIENT_CLASS_PROPERTY = "keyvalue.store.client.class";

    // Registry to cache client instances
    private static final ConcurrentHashMap<String, KeyValueStoreClient> clientRegistry = new ConcurrentHashMap<>();

    private KeyValueStoreManager() {
        // To prevent instantiation
    }

    /**
     * Gets an instance of a {@link KeyValueStoreClient} based on the system property.
     * Uses a registry pattern to cache and reuse instances.
     *
     * @return A configured and connected {@link KeyValueStoreClient} instance.
     * @throws KeyValueStoreException if the specified store type is unknown or if the
     *                                client adapter fails to connect.
     */
    public static KeyValueStoreClient getClient() throws KeyValueStoreException {
        String clientClassName = resolveClientClassName();
        
        if (clientRegistry.get(clientClassName) == null) {
            if (clientRegistry.get(clientClassName) == null) {
                if (clientClassName != null && !clientClassName.trim().isEmpty()) {
                    KeyValueStoreClient clientInstance;
                    try {
                        Class<?> clazz = Class.forName(clientClassName);

                        // Verify it implements KeyValueStoreClient interface
                        if (!KeyValueStoreClient.class.isAssignableFrom(clazz)) {
                            throw new KeyValueStoreException("Class " + clientClassName +
                                    " does not implement KeyValueStoreClient interface");
                        }

                        // Try default constructor first
                        Constructor<?> constructor = clazz.getDeclaredConstructor();
                        clientInstance = (KeyValueStoreClient) constructor.newInstance();

                        // Connect the client
                        clientInstance.connect();

                        clientRegistry.put(clientClassName, clientInstance);
                        log.info("Successfully created and connected KeyValueStoreClient: {}", clientClassName);

                    } catch (ClassNotFoundException | NoSuchMethodException |
                            IllegalAccessException | InstantiationException |
                            java.lang.reflect.InvocationTargetException e) {
                        throw new KeyValueStoreException("Error occurred while creating a KeyValueStoreClient of type " + clientClassName, e);
                    } catch (Exception e) {
                        throw new KeyValueStoreException("Error connecting KeyValueStoreClient of type " + clientClassName, e);
                    }

                    return clientInstance;
                }

                throw new KeyValueStoreException("Provided class name is either empty or null. Hence cannot create the KeyValueStoreClient.");
            }
        }

        KeyValueStoreClient clientInstance = clientRegistry.get(clientClassName);
        log.debug("KeyValueStoreClient of type {} is already created. Hence returning same instance", 
                clientInstance.getClass().toString().replaceAll("[\r\n]", ""));
        return clientInstance;
    }

    /**
     * Resolves the client class name based on system properties.
     */
    private static String resolveClientClassName() {
        String customClassName = System.getProperty(KEYVALUE_STORE_CLIENT_CLASS_PROPERTY,
                "org.wso2.carbon.apimgt.throttling.siddhi.extension.util.kvstore.RedisClientAdapter");

        if (customClassName != null && !customClassName.trim().isEmpty()) {
            log.debug("Using KeyValueStoreClient class: {}", customClassName);
            return customClassName.trim();
        }

        // Fallback (though we have a default above)
        String storeType = System.getProperty(KEYVALUE_STORE_TYPE_PROPERTY, DEFAULT_KV_STORE_TYPE).toLowerCase();
        switch (storeType) {
            case REDIS_TYPE:
                return "org.wso2.carbon.apimgt.throttling.siddhi.extension.util.kvstore.RedisClientAdapter";
            case VALKEY_TYPE:
                return "org.wso2.carbon.apimgt.throttling.siddhi.extension.util.kvstore.ValkeyClientAdapter";
            default:
                throw new KeyValueStoreException("Unknown key-value store type: " + storeType);
        }
    }

    /**
     * Checks if a client is currently cached and connected.
     */
    public static boolean isClientAvailable() {
        try {
            String clientClassName = resolveClientClassName();
            KeyValueStoreClient client = clientRegistry.get(clientClassName);
            return client != null && client.isConnected();
        } catch (Exception e) {
            log.debug("Error checking client availability", e);
            return false;
        }
    }

    /**
     * Shuts down resources associated with the configured key-value store clients.
     */
    public static void shutdown() {
        log.info("Shutting down KeyValueStoreManager managed resources.");
        
        synchronized (KeyValueStoreManager.class) {
            for (String className : clientRegistry.keySet()) {
                KeyValueStoreClient client = clientRegistry.get(className);
                if (client != null) {
                    try {
                        log.info("Disconnecting KeyValueStoreClient: {}", className);
                        client.disconnect();
                    } catch (Exception e) {
                        log.error("Error disconnecting KeyValueStoreClient: {}", className, e);
                    }
                }
            }
            clientRegistry.clear();
        }
        
        log.info("KeyValueStoreManager shutdown process completed.");
    }

    /**
     * Clears the cached client instances. Next call to getClient() will create a new instance.
     * Useful for testing or when configuration changes at runtime.
     */
    public static void clearCache() {
        log.info("Clearing KeyValueStoreClient cache");
        
        synchronized (KeyValueStoreManager.class) {
            for (String className : clientRegistry.keySet()) {
                KeyValueStoreClient client = clientRegistry.get(className);
                if (client != null) {
                    try {
                        client.disconnect();
                    } catch (Exception e) {
                        log.warn("Error disconnecting client during cache clear: {}", className, e);
                    }
                }
            }
            clientRegistry.clear();
        }
    }
}


//package org.wso2.siddhi.core.util.kvstore;
//
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
///**
// * A static manager that dynamically loads KeyValueStoreClient implementations
// * based on system properties.
// */
//public final class KeyValueStoreManager {
//
//    private static final Logger log = LoggerFactory.getLogger(org.wso2.siddhi.core.util.kvstore.KeyValueStoreManager.class);
//
//    // System property keys
//    public static final String KEYVALUE_STORE_TYPE_PROPERTY = "keyvalue.store.type";
//    public static final String KEYVALUE_STORE_CLIENT_CLASS_PROPERTY = "keyvalue.store.client.class";
//    public static final String DEFAULT_KV_STORE_TYPE = "redis";
//
//    private static volatile org.wso2.siddhi.core.util.kvstore.KeyValueStoreClient cachedClient;
//    private static final Object lock = new Object();
//
//    /**
//     * Private constructor to prevent instantiation of this utility class.
//     */
//    private KeyValueStoreManager() {
//    }
//
//    /**
//     * Gets an instance of a KeyValueStoreClient based on system properties.
//     * Uses lazy initialization and caches the client instance.
//     *
//     * @return A configured KeyValueStoreClient instance.
//     * @throws KeyValueStoreException if the client cannot be created.
//     */
//    public static org.wso2.siddhi.core.util.kvstore.KeyValueStoreClient getClient() throws KeyValueStoreException {
//        if (cachedClient == null) {
//            synchronized (lock) {
//                if (cachedClient == null) {
//                    cachedClient = createClient();
//                }
//            }
//        }
//        return cachedClient;
//    }
//
//    /**
//     * Creates a new KeyValueStoreClient instance based on system properties.
//     */
//    private static org.wso2.siddhi.core.util.kvstore.KeyValueStoreClient createClient() throws KeyValueStoreException {
//        try {
//            String clientClassName = resolveClientClassName();
//            log.info("Loading KeyValueStoreClient implementation: {}", clientClassName);
//
//            // Load the class using classloader
//            ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
//            if (classLoader == null) {
//                classLoader = org.wso2.siddhi.core.util.kvstore.KeyValueStoreManager.class.getClassLoader();
//            }
//
//            Class<?> clientClass = classLoader.loadClass(clientClassName);
//
//            // Verify it implements KeyValueStoreClient interface
//            if (!org.wso2.siddhi.core.util.kvstore.KeyValueStoreClient.class.isAssignableFrom(clientClass)) {
//                throw new KeyValueStoreException("Class " + clientClassName +
//                        " does not implement KeyValueStoreClient interface");
//            }
//
//            // Create instance using default constructor
//            Object clientInstance = clientClass.getDeclaredConstructor().newInstance();
//            org.wso2.siddhi.core.util.kvstore.KeyValueStoreClient client = (org.wso2.siddhi.core.util.kvstore.KeyValueStoreClient) clientInstance;
//
//            // Connect the client
//            client.connect();
//
//            log.info("Successfully created and connected KeyValueStoreClient: {}", clientClassName);
//            return client;
//
//        } catch (ClassNotFoundException e) {
//            throw new KeyValueStoreException("KeyValueStoreClient implementation class not found", e);
//        } catch (InstantiationException | IllegalAccessException e) {
//            throw new KeyValueStoreException("Failed to instantiate KeyValueStoreClient", e);
//        } catch (NoSuchMethodException e) {
//            throw new KeyValueStoreException("No default constructor found for KeyValueStoreClient", e);
//        } catch (Exception e) {
//            throw new KeyValueStoreException("Error creating KeyValueStoreClient", e);
//        }
//    }
//
//    /**
//     * Resolves the client class name based on system properties.
//     */
//    private static String resolveClientClassName() {
//        // First check if a custom client class is directly specified
//        //        String customClassName = System.getProperty(KEYVALUE_STORE_CLIENT_CLASS_PROPERTY);
//        String customClassName = System.getProperty(KEYVALUE_STORE_CLIENT_CLASS_PROPERTY,
//                "org.wso2.carbon.apimgt.throttling.siddhi.extension.util.kvstore.RedisClientAdapter");
//
//        if (customClassName != null && !customClassName.trim().isEmpty()) {
//            log.info("Using custom KeyValueStoreClient class: {}", customClassName);
//            return customClassName.trim();
//        }
//
//        // Otherwise, resolve based on store type
//        String storeType = System.getProperty(KEYVALUE_STORE_TYPE_PROPERTY, DEFAULT_KV_STORE_TYPE).toLowerCase();
//        log.info("Resolving KeyValueStoreClient for store type: {}", storeType);
//
//        return getDefaultClientClassName(storeType);
//    }
//
//    /**
//     * Maps store type to default client implementation class name.
//     */
//    private static String getDefaultClientClassName(String storeType) {
//        switch (storeType) {
//        case "redis":
//            return System.getProperty("keyvalue.store.redis.class",
//                    "org.wso2.siddhi.core.util.kvstore.RedisClientAdapter");
//        case "valkey":
//            return System.getProperty("keyvalue.store.valkey.class",
//                    "org.wso2.siddhi.core.util.kvstore.ValkeyClientAdapter");
//        case "inmemory":
//            return System.getProperty("keyvalue.store.inmemory.class",
//                    "org.wso2.siddhi.core.util.kvstore.InMemoryClientAdapter");
//        case "hazelcast":
//            return System.getProperty("keyvalue.store.hazelcast.class",
//                    "org.wso2.siddhi.core.util.kvstore.HazelcastClientAdapter");
//        default:
//            throw new KeyValueStoreException("Unsupported store type: " + storeType +
//                    ". Supported types: redis, valkey, inmemory, hazelcast. " +
//                    "Or specify a custom class using: " + KEYVALUE_STORE_CLIENT_CLASS_PROPERTY);
//        }
//    }
//
//    /**
//     * Shuts down the cached client and clears the cache.
//     */
//    public static void shutdown() {
//        synchronized (lock) {
//            if (cachedClient != null) {
//                try {
//                    log.info("Shutting down KeyValueStoreClient: {}", cachedClient.getClass().getName());
//                    cachedClient.disconnect();
//                } catch (Exception e) {
//                    log.error("Error during KeyValueStoreClient shutdown", e);
//                } finally {
//                    cachedClient = null;
//                }
//            }
//        }
//    }
//
//    /**
//     * Clears the cached client instance. Next call to getClient() will create a new instance.
//     * Useful for testing or when configuration changes at runtime.
//     */
//    public static void clearCache() {
//        synchronized (lock) {
//            if (cachedClient != null) {
//                try {
//                    cachedClient.disconnect();
//                } catch (Exception e) {
//                    log.warn("Error disconnecting cached client during cache clear", e);
//                }
//                cachedClient = null;
//            }
//        }
//    }
//
//    /**
//     * Checks if a client is currently cached and connected.
//     */
//    public static boolean isClientAvailable() {
//        org.wso2.siddhi.core.util.kvstore.KeyValueStoreClient client = cachedClient;
//        return client != null && client.isConnected();
//    }
//}