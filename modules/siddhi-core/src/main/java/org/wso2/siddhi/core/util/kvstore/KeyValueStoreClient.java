package org.wso2.siddhi.core.util.kvstore;

/**
 * Interface for a generic Key-Value store client.
 */
public interface KeyValueStoreClient {

    /**
     * Establishes a connection to the key-value store.
     */
    void connect();

    /**
     * Disconnects from the key-value store and releases any associated resources.
     * This can involve closing active connections, shutting down a connection pool,
     * or other cleanup tasks.
     */
    void disconnect();

    /**
     * Checks if the client is currently connected or able to establish a connection
     * to the key-value store.

     */
    boolean isConnected();

    /**
     * Retrieves the string value associated with the given key.
     *
     * @param key The key whose associated value is to be returned.
     */
    String get(String key);

    /**
     * Sets the string value for the given key.
     * If the store previously contained a mapping for the key, the old value is replaced by
     * the specified value.
     *
     * @param key   The key with which the specified value is to be associated.
     * @param value The value to be associated with the specified key.
     */
    void set(String key, String value);

    /**
     * Increments the numeric value of a key by one.
     * If the key does not exist, it is set to 0 before performing the operation.
     *
     * @param key The key whose numeric value is to be incremented.
     * @return The value of the key after the increment operation.
     */
    long incrementBy(String key, long increment);

    /**
     * Decrements the numeric value of a key by one.
     * If the key does not exist, it is set to 0 before performing the operation.
     *
     * @param key The key whose numeric value is to be decremented.
     * @return The value of the key after the decrement operation.
     */
    long decrementBy(String key, long decrement);

    /**
     * Deletes the mapping for a key from this store if it is present.
     *
     * @param key The key whose mapping is to be removed from the store.
     */
    void delete(String key);
}
