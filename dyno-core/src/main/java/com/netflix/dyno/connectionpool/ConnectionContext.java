package com.netflix.dyno.connectionpool;

import java.util.Map;

/**
 * Context specific to a connection.  This interface makes it possible to store
 * connection specific state such as prepared CQL statement ids etc.
 * 
 * @author poberai
 *
 */
public interface ConnectionContext {
    /**
     * Set metadata identified by 'key'
     * @param key
     * @param obj
     */
    public void setMetadata(String key, Object obj);
    
    /**
     * @return Get metadata stored by calling setMetadata
     * @param key
     */
    public Object getMetadata(String key);
    
    /**
     * @return Return true if the metadata with the specified key exists.
     * @param key
     */
    public boolean hasMetadata(String key);
    
    /**
     * Reset all metadata
     */
    public void reset();
    
    /**
     * Return all context
     * @return Map<String, Object>
     */
    public Map<String, Object> getAll();
}
