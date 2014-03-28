package com.netflix.dyno.connectionpool.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.dyno.connectionpool.Connection;
import com.netflix.dyno.connectionpool.ConnectionFactory;
import com.netflix.dyno.connectionpool.ConnectionPoolConfiguration;
import com.netflix.dyno.connectionpool.ConnectionPoolMonitor;
import com.netflix.dyno.connectionpool.Host;
import com.netflix.dyno.connectionpool.HostConnectionPool;
import com.netflix.dyno.connectionpool.exception.DynoConnectException;
import com.netflix.dyno.connectionpool.exception.DynoException;
import com.netflix.dyno.connectionpool.exception.IsDeadConnectionException;
import com.netflix.dyno.connectionpool.exception.PoolTimeoutException;
import com.netflix.dyno.connectionpool.exception.ThrottledException;
import com.netflix.dyno.connectionpool.exception.TimeoutException;


/**
 * Pool of connections for a single host and implements the {@link HostConnectionPool} interface
 * 
 * <p>
 * <b>Salient Features </b> <br/> <br/>
 *   
 *      This class also provides an async mechanism to create / prime and borrow {@link Connection}(s) using a {@link LinkedBlockingQueue} <br/>
 *      Clients borrowing connections can wait at the end of the queue for a connection to be available. They send a {@link SimpleHostConnectionPool#tryOpenAsync()} request 
 *      to create a new connection before waiting, but don't necessarily wait for the same connection to be opened, since they could be unblocked by 
 *      another client returning a previously used {@link Connection}
 *      
 *      The class also provides a {@link SimpleHostConnectionPool#markAsDown(ConnectionException)} method which helps purge all connections and then
 *      attempts to init a new set of connections to the host. 
 *      
 * </p>
 * 
 * @author poberai
 * 
 */
public class SimpleHostConnectionPool<CL> implements HostConnectionPool<CL> {
	
    private final static Logger LOG = LoggerFactory.getLogger(SimpleHostConnectionPool.class);
    
    private final static int MAX_PRIME_CONNECTIONS_RETRY_ATTEMPT = 2;
    private final static int PRIME_CONNECTION_DELAY = 100;

    /**
     * Interface to notify the owning connection pool of up/down state changes.
     * This give the owning connection pool a chance to remove a downed host
     * from its internal state until it's back up.
     * 
     * @author poberai
     * 
     * @param <CL>
     */
    public interface Listener<CL> {
        void onHostDown(HostConnectionPool<CL> pool);

        void onHostUp(HostConnectionPool<CL> pool);
    }

    private static final AtomicLong             poolIdCounter = new AtomicLong(0);
    private final long                          id = poolIdCounter.incrementAndGet();

    private final BlockingQueue<Connection<CL>> availableConnections;

    private final AtomicInteger                 activeCount          = new AtomicInteger(0);
    private final AtomicInteger                 pendingConnections   = new AtomicInteger(0);
    private final AtomicInteger                 blockedThreads       = new AtomicInteger(0);
    private final AtomicInteger                 openConnections      = new AtomicInteger(0);
    private final AtomicInteger                 failedOpenConnections= new AtomicInteger(0);
    private final AtomicInteger                 closedConnections    = new AtomicInteger(0);
    private final AtomicLong                    borrowedCount        = new AtomicLong(0);
    private final AtomicLong                    returnedCount        = new AtomicLong(0);
    private final AtomicInteger                 connectAttempt       = new AtomicInteger(0);
    private final AtomicInteger                 markedDownCount      = new AtomicInteger(0);
    
    private final AtomicInteger                 errorsSinceLastSuccess = new AtomicInteger(0);

    private final ConnectionFactory<CL>         factory;
    private final Host                          host;
    private final AtomicBoolean                 isShutdown           = new AtomicBoolean(false);
    private final AtomicBoolean                 isReconnecting       = new AtomicBoolean(false);
    
//    private final ScheduledExecutorService      executor;
    
//    private final RetryBackoffStrategy.Instance retryContext;
//    private final BadHostDetector.Instance      badHostDetector;
//    private final LatencyScoreStrategy.Instance latencyStrategy;
    
    private final Listener<CL>                  listener;
    private final ConnectionPoolMonitor         monitor;

    protected final ConnectionPoolConfiguration config;
    
    private final ConnectionPoolState state = new ConnectionPoolState();

    public SimpleHostConnectionPool(Host host, ConnectionFactory<CL> factory, ConnectionPoolMonitor monitor,
            ConnectionPoolConfiguration config, Listener<CL> listener) {
        
        this.host            = host;
        this.config          = config;
        this.factory         = factory;
        this.listener        = listener;

//        this.retryContext    = config.getRetryBackoffStrategy().createInstance();
//        this.latencyStrategy = config.getLatencyScoreStrategy().createInstance();
//        this.badHostDetector = config.getBadHostDetector().createInstance();

        this.monitor         = monitor;
        this.availableConnections = new LinkedBlockingQueue<Connection<CL>>();

//        this.executor        = config.getHostReconnectExecutor();
//        Preconditions.checkNotNull(config.getHostReconnectExecutor(), "HostReconnectExecutor cannot be null");
    }


    /**
     * Create a connection as long the max hasn't been reached
     * 
     * @param timeout
     *            - Max wait timeout if max connections have been allocated and
     *            pool is empty. 0 to throw a MaxConnsPerHostReachedException.
     * @return
     * @throws TimeoutException
     *             if timeout specified and no new connection is available
     *             MaxConnsPerHostReachedException if max connections created
     *             and no timeout was specified
     */
    @Override
    public Connection<CL> borrowConnection(int timeout) throws DynoException {
    	
        Connection<CL> connection = null;
        long startTime = System.currentTimeMillis();
        
        try {
            // Try to get a free connection without blocking.
            connection = availableConnections.poll();
            if (connection != null) {
                return connection;
            }

            // shoot off a request to open a new connection
            boolean isOpenning = tryOpenAsync();

            // Wait for a connection to free up or a new one to be opened
            if (timeout > 0) {
                connection = waitForConnection(isOpenning ? config.getConnectTimeout() : timeout);
                return connection;
            } else {
                throw new PoolTimeoutException("Fast fail waiting for connection from pool")
                        .setHost(getHost())
                        .setLatency(System.currentTimeMillis() - startTime);
            }
        } finally {
            if (connection != null) {
            	state.trackConnectionBorrowed(System.currentTimeMillis() - startTime);
            }
        }
    }
    

    /**
     * Try to open a new connection asynchronously. We don't actually return a
     * connection here. Instead, the connection will be added to idle queue when
     * it's ready.
     */
    private boolean tryOpenAsync() {
    	
    	if (!state.canOpenNewConnection()) {
    		return false;
    	}
    	
        Connection<CL> connection = null;
        // Try to open a new connection, as long as we haven't reached the max
        boolean allowedToOpenConn = state.attemptNewConnectionOpen();
        
        if (allowedToOpenConn) {
            
        	try {
        		connection = factory.createConnection(this);
        	} catch (ThrottledException e) {
        		// opening way too many conns
        		state.trackNewConnectionOpenThrottled();
        		return false;
        	}
        	
            connection.openAsync(new Connection.AsyncOpenCallback<CL>() {
                @Override
                public void success(Connection<CL> connection) {
                	
                	state.trackNewConnectionOpenedSuccessfully();
                    availableConnections.add(connection);

                    // Sanity check in case the connection
                    // pool was closed
                    if (isShutdown()) {
                        discardIdleConnections();
                    }
                }

                @Override
                public void failure(Connection<CL> conn, DynoConnectException e) {
                	
                	state.trackNewConnectionOpenFailed();

                    if (e instanceof IsDeadConnectionException) {
                        noteError(e);
                    }
                }
            });
            
            return true;

        } else {
        	return false;
        }
    }
    
    /**
     * Internal method to wait for a connection from the available connection
     * pool.
     * 
     * @param timeout
     * @return
     * @throws ConnectionException
     */
    private Connection<CL> waitForConnection(int timeout) throws DynoConnectException {
        
    	Connection<CL> connection = null;
        long startTime = System.currentTimeMillis();
        
        try {
        	state.trackWaitingOnConnection();
            
            connection = availableConnections.poll(timeout, TimeUnit.MILLISECONDS);
            if (connection != null) {
                return connection;
            }
            
            throw new PoolTimeoutException("Timed out waiting for connection")
                .setHost(getHost())
                .setLatency(System.currentTimeMillis() - startTime);
            
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new DynoConnectException("Thread interrupted waiting for connection")
                .setHost(getHost())
                .setLatency(System.currentTimeMillis() - startTime);
        }
        finally {
        	state.trackDoneWaitingOnConnection();
        }
    }

    /**
     * Return a connection to this host
     * 
     * @param connection
     */
    @Override
    public boolean returnConnection(Connection<CL> connection) {
    	
    	boolean shouldCloseConnection = state.trackConnectionReturned();
    	
        DynoConnectException ce = connection.getLastException();
        if (ce != null) {
            if (ce instanceof IsDeadConnectionException) {
                noteError(ce);
                internalCloseConnection(connection);
                return true;
            }
        }
        errorsSinceLastSuccess.set(0);

        // Still within the number of max active connection
        if (!shouldCloseConnection) {
            availableConnections.add(connection);

            if (isShutdown()) {
                discardIdleConnections();
                return true;
            }
        } else {
            // maxConnsPerHost was reduced. This may end up closing too many
            // connections, but that's ok. We'll open them later.
            internalCloseConnection(connection);
            return true;
        }

        return false;
    }

    @Override
    public boolean closeConnection(Connection<CL> connection) {
    	state.trackConnectionReturned();
        internalCloseConnection(connection);
        return true;
    }

    private void internalCloseConnection(Connection<CL> connection) {
        try {
            connection.close();
        } finally {
        	state.trackConnectionClose();
        }
    }

    private void noteError(DynoConnectException reason) {
        if (errorsSinceLastSuccess.incrementAndGet() > 3) 
            markAsDown(reason);
    }
 

    @Override
    public void shutdown() {
        isReconnecting.set(true);
        isShutdown.set(true);
        
        discardIdleConnections();
//        config.getLatencyScoreStrategy().removeInstance(this.latencyStrategy);
//        config.getBadHostDetector().removeInstance(this.badHostDetector);
    }



    @Override
    public boolean isShutdown() {
        return isShutdown.get();
    }

    public boolean isReconnecting() {
        return isReconnecting.get();
    }
    
    @Override
    public Host getHost() {
        return host;
    }


    /**
     * Drain all idle connections and close them.  Connections that are currently borrowed
     * will not be closed here.
     */
    private void discardIdleConnections() {
        
    	List<Connection<CL>> connections = new ArrayList<Connection<CL>>();
        availableConnections.drainTo(connections);

        for (Connection<CL> connection : connections) {
            try {
                connection.close(); // This is usually an async operation
            } catch (Throwable t) {
            	// TODO
            }
        }
        
        state.trackDiscardedIdleConnections(connections.size());
    }

    public String toString() {
    	return state.toString();
    }

    @Override
    public boolean isActive() {
        return !this.isShutdown.get();
    }


    /**
     * Mark the host as down. No new connections will be created from this host.
     * Connections currently in use will be allowed to continue processing.
     */
    @Override
    public void markAsDown(DynoException reason) {

//    	// Make sure we're not triggering the reconnect process more than once
//        if (isReconnecting.compareAndSet(false, true)) {
//            
//            markedDownCount.incrementAndGet();
//            
//            if (reason != null && !(reason instanceof TimeoutException)) {
//                discardIdleConnections();
//            }
//            
//            listener.onHostDown(this);
//            monitor.hostDown(getHost(), reason);
//
//            retryContext.begin();
//            
//            try {
//                long delay = retryContext.getNextDelay();
//                executor.schedule(new Runnable() {
//                    @Override
//                    public void run() {
//                        Thread.currentThread().setName("RetryService : " + host.getName());
//                        try {
//                            if (activeCount.get() == 0)
//                                reconnect();
//                            
//                            // Created a new connection successfully.
//                            try {
//                                retryContext.success();
//                                if (isReconnecting.compareAndSet(true, false)) {
//                                    monitor .onHostReactivated(host, SimpleHostConnectionPool.this);
//                                    listener.onHostUp(SimpleHostConnectionPool.this);
//                                }
//                            }
//                            catch (Throwable t) {
//                                LOG.error("Error reconnecting client", t);
//                            }
//                            return;
//                        }
//                        catch (Throwable t) {
//                            // Ignore
//                            //t.printStackTrace();
//                        }
//                        
//                        if (!isShutdown()) {
//                            long delay = retryContext.getNextDelay();
//                            executor.schedule(this, delay, TimeUnit.MILLISECONDS);
//                        }
//                    }
//                }, delay, TimeUnit.MILLISECONDS);
//            }
//            catch (Exception e) {
//                LOG.error("Failed to schedule retry task for " + host.getHostName(), e);
//            }
//        }
    }


    @Override
    public int primeConnections(int numConnections) throws DynoException, InterruptedException {
    	throw new RuntimeException("not implemented");
//        if (isReconnecting()) {
//            throw new DynoException("Can't prime connections on downed host.");
//        }
//        // Don't try to create more than we're allowed
//        int remaining = Math.min(numConnections, config.getMaxConnsPerHost() - getActiveConnectionCount());
//        
//        // Attempt to open 'count' connections and allow for MAX_PRIME_CONNECTIONS_RETRY_ATTEMPT
//        // retries before giving up if we can't open more.
//        int opened = 0;
//        int reconnectAttempt = 0;
//        
//        Exception lastException = null;
//        while ((opened < remaining) && (reconnectAttempt < MAX_PRIME_CONNECTIONS_RETRY_ATTEMPT)) {
//            try {
//                reconnect();
//                opened++;
//            }
//            catch (Exception e) {
//                lastException = e;
//                Thread.sleep(PRIME_CONNECTION_DELAY);
//                reconnectAttempt++;
//            }
//        }
//        
//        // If no connection was opened then mark this host as down
//        if (remaining > 0 && opened == 0) {
//            this.markAsDown(new DynoException(lastException));
//            throw new DynoException("Failed to prime connections", lastException);
//        }
//        return opened;
    }
    
//    
//    private void reconnect() throws Exception {
//        try {
//            if (activeCount.get() < config.getMaxConnsPerHost()) {
//                if (activeCount.incrementAndGet() <= config.getMaxConnsPerHost()) {
//                    connectAttempt.incrementAndGet();
//                    Connection<CL> connection = factory.createConnection(SimpleHostConnectionPool.this);
//                    connection.open();
//                    
//                    errorsSinceLastSuccess.set(0);
//                    availableConnections.add(connection);
//                    openConnections.incrementAndGet();
//                }
//                else {
//                    activeCount.decrementAndGet();
//                }
//            }
//        } catch (ConnectionException e) {
//            failedOpenConnections.incrementAndGet();
//            activeCount.decrementAndGet();
//            noteError(e);
//            throw e;
//        } catch (Throwable t) {
//            failedOpenConnections.incrementAndGet();
//            activeCount.decrementAndGet();
//            ConnectionException ce = new UnknownException(t);
//            noteError(ce);
//            throw ce;
//        }
//    }
//    
    private class ConnectionPoolState { 
    	
    	private ConnectionPoolState() {
    	}
    	
    	public void trackDiscardedIdleConnections(int size) {
            activeCount.addAndGet(-size);
            closedConnections.addAndGet(size);
		}

		private final AtomicInteger activeCount = new AtomicInteger(0);
    	private final AtomicInteger borrowedCount = new AtomicInteger(0);
    	private final AtomicInteger throttledCount = new AtomicInteger(0);
    	private final AtomicInteger blockedThreads = new AtomicInteger(0);
    	private final AtomicInteger returnedCount = new AtomicInteger(0);
    	

    	private void trackConnectionBorrowed(long delay) {
    		borrowedCount.incrementAndGet();
    		monitor.incConnectionBorrowed(host, delay);
    	}
    	
    	public boolean trackConnectionReturned() {
			returnedCount.incrementAndGet();
	        monitor.incConnectionReturned(host);
	        
	        return activeCount.get() > config.getMaxConnsPerHost();
		}
    	
    	private boolean canOpenNewConnection() {
    		return activeCount.get() < config.getMaxConnsPerHost();
    	}
    
    	private boolean attemptNewConnectionOpen() {
    		
    		if (activeCount.incrementAndGet() <= config.getMaxConnsPerHost()) {
    		
    			if (pendingConnections.incrementAndGet() > config.getMaxPendingConnectionsPerHost()) {
    				
        			// Don't try to open too many connections at the same time.
    				pendingConnections.decrementAndGet();
    				activeCount.decrementAndGet(); 
    				return false;
    				
    			} else {
    				// YES, we can open a new connection
    				return true;
    			}
    		} else {
    			// someone already beat up to maxing out the quota. 
    			activeCount.decrementAndGet();
    			return false;
    		}
    	}
    	
    	private void trackNewConnectionOpenedSuccessfully() {
            pendingConnections.decrementAndGet();
    	}

    	private void trackNewConnectionOpenFailed() {
            failedOpenConnections.incrementAndGet();
            pendingConnections.decrementAndGet();
            activeCount.decrementAndGet();
    	}

    	private void trackNewConnectionOpenThrottled() {
    		throttledCount.incrementAndGet();
            pendingConnections.decrementAndGet();
            activeCount.decrementAndGet();
    	}
    	
    	public void trackDoneWaitingOnConnection() {
    		blockedThreads.decrementAndGet();
		}

		public void trackWaitingOnConnection() {
			blockedThreads.incrementAndGet();
		}
		
		private void trackConnectionClose() {
			closedConnections.incrementAndGet();
			activeCount.decrementAndGet();
		}
		
	    public String toString() {
	        int idle = availableConnections.size();
	        int open = activeCount.get();
	        return new StringBuilder()
	                .append("SimpleHostConnectionPool[")
	                .append("host="    ).append(host).append("-").append(id)
	                .append(",down="   ).append(markedDownCount.get())
	                .append(",active=" ).append(!isShutdown())
	                .append(",recon="  ).append(isReconnecting())
	                .append(",connections(")
	                .append(  "open="  ).append(open)
	                .append( ",idle="  ).append(idle)
	                .append( ",busy="  ).append(open - idle)
	                .append( ",closed=").append(closedConnections.get())
	                .append( ",failed=").append(failedOpenConnections.get())
	                .append(")")
	                .append(",borrow=" ).append(borrowedCount.get())
	                .append(",return=" ).append(returnedCount.get())
	                .append(",blocked=").append(blockedThreads.get())
	                .append(",pending=").append(pendingConnections.get())
	                .append("]").toString();
	    }
    }
}


//@Override
//public int getActiveConnectionCount() {
//  return activeCount.get();
//}
//
//@Override
//public int getIdleConnectionCount() {
//  return availableConnections.size();
//}
//
//@Override
//public int getPendingConnectionCount() {
//  return pendingConnections.get();
//}
//
//@Override
//public int getBlockedThreadCount() {
//  return blockedThreads.get();
//}
//
//@Override
//public int getOpenedConnectionCount() {
//  return openConnections.get();
//}
//
//@Override
//public int getFailedOpenConnectionCount() {
//  return failedOpenConnections.get();
//}
//
//@Override
//public int getClosedConnectionCount() {
//  return closedConnections.get();
//}

//
//
//@Override
//public int getBusyConnectionCount() {
//  return getActiveConnectionCount() - getIdleConnectionCount() - getPendingConnectionCount();
//}
//
//@Override
//public double getScore() {
//  return latencyStrategy.getScore();
//}
//
//@Override
//public void addLatencySample(long latency, long now) {
//  latencyStrategy.addSample(latency);
//}
//
//@Override
//public int getErrorsSinceLastSuccess() {
//  return errorsSinceLastSuccess.get();
//}
