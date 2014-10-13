package com.netflix.dyno.memcache;

import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import net.spy.memcached.DefaultConnectionFactory;
import net.spy.memcached.KetamaNodeLocator;
import net.spy.memcached.MemcachedNode;
import net.spy.memcached.NodeLocator;

import com.netflix.dyno.connectionpool.ConnectionPoolConfiguration;
import com.netflix.dyno.connectionpool.ConnectionPoolMonitor;
import com.netflix.dyno.connectionpool.Host;

/**
 * This class encapsulates a custom {@link SpyMemcachedRRLocator} for our custom local zone aware round robin load balancing
 * with RR lb over the remote zone for fallback cases. 
 * 
 * @author poberai
 */
public class SpyMemcachedConnectionFactory extends DefaultConnectionFactory {
	
	private final String localDC; 
	private final InnerState innerState;
	private final ConnectionPoolConfiguration cpConfig;
	private final ConnectionPoolMonitor cpMonitor;

	/**
	 * Constructor
	 * @param hosts
	 * @param config
	 * @param monitor
	 */
	public SpyMemcachedConnectionFactory(List<Host> hosts, ConnectionPoolConfiguration config, ConnectionPoolMonitor monitor) {
		super();
		this.localDC = System.getenv("EC2_AVAILABILITY_ZONE");
		this.innerState = new InnerState(hosts);
		this.cpConfig = config;
		this.cpMonitor = monitor;
	}

	/**
	 * returns a instance of {@link KetamaNodeLocator}.
	 */
	public NodeLocator createLocator(List<MemcachedNode> list) {
		
		return new TokenAwareLocator(this);
//			return new RoundRobinLocator(this);
	}

	public MemcachedNode getMemcachedNodeForHost(Host host) {
		return innerState.saToMCNodeMap.get(host.getSocketAddress());
	}

	public Host getHostForMemcachedNode(MemcachedNode node) {
		return innerState.saToHostMap.get(node.getSocketAddress());
	}
	
	public Collection<MemcachedNode> getAllNodes() {
		return innerState.allMCNodes;
	}
	

	public Collection<MemcachedNode> getAllLocalZoneNodes() {
		return innerState.localZoneMCNodes;
	}

	public Collection<MemcachedNode> getAllRemoteZoneNodes() {
		return innerState.remoteZoneMCNodes;
	}
	
	public ConnectionPoolMonitor getCPMonitor() {
		return cpMonitor;
	}
	
	public ConnectionPoolConfiguration getCPConfig() {
		return cpConfig;
	}
	public static abstract class InstrumentedLocator implements NodeLocator {
		
		private final SpyMemcachedConnectionFactory connFactory; 
		
		public InstrumentedLocator(SpyMemcachedConnectionFactory cFactory) {
			this.connFactory = cFactory;
		}

		public abstract MemcachedNode getPrimaryNode(String key);
		
		public abstract Iterator<MemcachedNode> getNodeSequence(String key);

		public SpyMemcachedConnectionFactory getConnectionFactory() {
			return connFactory;
		}
		
		@Override
		public MemcachedNode getPrimary(String k) {
			
			MemcachedNode node = null;
			try {
				node = getPrimaryNode(k);
				
				if (node == null) {
					System.out.println("\n\nBorrowing NULL primary node: " + node + "\n\n");
				}
				return node;
			} finally {
				
				// record stats for this host
				if (node != null) {
					Host host = connFactory.getHostForMemcachedNode(node);
					if (host != null) {
						connFactory.getCPMonitor().incConnectionBorrowed(host, -1);
					}
				}
			}
		}
		
		/**
		 * Simple wrapper around Iterator<MemcachedNode> so that we can track connection pool stats
		 */
		@Override
		public Iterator<MemcachedNode> getSequence(String k) {
			
			final Iterator<MemcachedNode> nodeIter = this.getNodeSequence(k);
			
			return new Iterator<MemcachedNode>() {

				@Override
				public boolean hasNext() {
					return nodeIter.hasNext();
				}

				@Override
				public MemcachedNode next() {
					
					MemcachedNode node = null;
					try {
						node = nodeIter.next();
						return node;
						
					} finally {
						// record this for stats
						if (node != null) {
							Host host = connFactory.getHostForMemcachedNode(node);
							if (host != null) {
								connFactory.getCPMonitor().incFailover(host, null);
							}
						}
					}
				}

				@Override
				public void remove() {
					throw new RuntimeException("Not implemented");
				}
			};
		}

		@Override
		public Collection<MemcachedNode> getAll() {
			return connFactory.getAllNodes();
		}

		@Override
		public NodeLocator getReadonlyCopy() {
			return this;
		}

		@Override
		public void updateLocator(List<MemcachedNode> nodes) {
			// do nothing
		}

	}
	
	
	/**
	 * Inner state tracking the local zone nodes in a circular list for RR load balancing. 
	 * It also tracks the remote zone nodes to fall back to during problems. 
	 * It also maintains a mapping of {@link SocketAddress} for all the {@link MemcachedNode}s
	 * This helps us connect {@link MemcachedNode}s to {@link Host}s and we can then track metrics with the 
	 * {@link ConnectionPoolMonitor} for each {@link Host} when we route requests to each {@link MemcachedNode}
	 *
	 * @author poberai
	 *
	 */
	private class InnerState { 


		private final List<MemcachedNode> allMCNodes = new ArrayList<MemcachedNode>();
		// Used to lookup the primary node for an operation
		private final List<MemcachedNode> localZoneMCNodes = new ArrayList<MemcachedNode>();
		// used to lookup the backup nodes
		private final List<MemcachedNode> remoteZoneMCNodes = new ArrayList<MemcachedNode>();

		// Maps to go from Host -> Node and vice versa
		private final ConcurrentHashMap<SocketAddress, MemcachedNode> saToMCNodeMap = new ConcurrentHashMap<SocketAddress, MemcachedNode>();
		private final ConcurrentHashMap<SocketAddress, Host> saToHostMap = new ConcurrentHashMap<SocketAddress, Host>();
		
		private InnerState() {
		}

		private InnerState(Collection<Host> hosts) {
			
			for (Host host : hosts) {
				saToHostMap.put(host.getSocketAddress(), host);
			}
		}

		private void updateMemcachedNodes(Collection<MemcachedNode> nodes) {

			for (MemcachedNode node : nodes) {

				allMCNodes.add(node);
				
				SocketAddress sa = node.getSocketAddress();
				saToMCNodeMap.put(sa, node);
				
				Host host = saToHostMap.get(sa);
				if (host == null) {
					throw new RuntimeException("Host not found for sa: " + sa + 
							", all hosts currently tracked: " + saToHostMap.values().toString());
				}
				
				if (cpConfig.localDcAffinity()) {
					if (host.getRack().equalsIgnoreCase(localDC)) {
						localZoneMCNodes.add(node);
					} else {
						// This is a remote zone host
						remoteZoneMCNodes.add(node);
					}
				} else { 
					// add to both 
					localZoneMCNodes.add(node);
					remoteZoneMCNodes.add(node);
				}
			}
		}
	}
	
}
