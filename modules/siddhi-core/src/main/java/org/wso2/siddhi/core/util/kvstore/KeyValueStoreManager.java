package org.wso2.siddhi.core.util.kvstore;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.lang.reflect.Constructor;

/**
 * Manages the creation of Key-Value store client instances based on system configuration.
 */
public class KeyValueStoreManager {

    private static final Logger log = LoggerFactory.getLogger(KeyValueStoreManager.class);
    public static final String kvStoreType = System.getProperty("distributed.throttle.type");
    public static final String REDIS_TYPE = "redis";
    private static volatile KeyValueStoreClient clientInstance;

    private KeyValueStoreManager() {
        // To prevent instantiation
    }

    /**
     * Gets the singleton KeyValueStoreClient instance.
     */
    public static KeyValueStoreClient getClient() throws KeyValueStoreException {
        if (clientInstance == null) {
            synchronized (KeyValueStoreManager.class) {
                if (clientInstance == null) {
                    clientInstance = createClient();
                }
            }
        }
        return clientInstance;
    }

    private static KeyValueStoreClient createClient() throws KeyValueStoreException {
        String clientClassName = resolveClientClassName();
        
        try {
            Class<?> clazz = Class.forName(clientClassName);
            
            if (!KeyValueStoreClient.class.isAssignableFrom(clazz)) {
                throw new KeyValueStoreException("Class " + clientClassName + 
                    " does not implement KeyValueStoreClient interface");
            }
            
            Constructor<?> constructor = clazz.getDeclaredConstructor();
            
            return (KeyValueStoreClient) constructor.newInstance();
            
        } catch (Exception e) {
            throw new KeyValueStoreException("Error creating KeyValueStoreClient: " + clientClassName, e);
        }
    }

    /**
     * Resolves the client class name based on system properties.
     */
    private static String resolveClientClassName() {
        if (kvStoreType == null) {
            throw new KeyValueStoreException("The key value store type is null");
        }
        
        if (REDIS_TYPE.equals(kvStoreType)) {
            return "org.wso2.carbon.apimgt.throttling.siddhi.extension.util.kvstore.JedisKeyValueStoreClient";
        }
        
        return kvStoreType; // Allow custom class names
    }

    /**
     * Shuts down resources associated with the configured key-value store clients.
     */
    public static void shutdown() {
        synchronized (KeyValueStoreManager.class) {
            if (clientInstance != null) {
                try {
                    log.info("Disconnecting KeyValueStoreClient");
                    clientInstance.disconnect();
                } catch (Exception e) {
                    log.error("Error disconnecting KeyValueStoreClient", e);
                } finally {
                    clientInstance = null;
                }
            }
        }
        log.info("KeyValueStoreManager shutdown completed.");
    }
}
