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

    public static final String kvStoreType = System.getProperty("distributed.throttle.type");
    public static final String REDIS_TYPE = "redis";
    public static final String DEFAULT_KV_STORE_TYPE = REDIS_TYPE;

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
        if (kvStoreType != null) {
            if (kvStoreType.equals(REDIS_TYPE)) {
                return "org.wso2.carbon.apimgt.throttling.siddhi.extension.util.kvstore.RedisClientAdapter";
            }
            return kvStoreType;
        }
        throw new KeyValueStoreException("The key value store value is null");

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
