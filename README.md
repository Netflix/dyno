# Dyno
 
 Dyno encapsulates features necessary to scale a client application utilizing [Dynomite](https://github.com/Netflix/dynomite).
 
 See the [blog post](http://techblog.netflix.com/2014/11/introducing-dynomite.html) for introductory info.
 
 See the [wiki](https://github.com/Netflix/dyno/wiki) for documentation and examples.

## Dyno Client Features

+ Connection pooling of persistent connections - this helps reduce connection churn on the Dynomite server with client connection reuse.
+ Topology aware load balancing (Token Aware) for avoiding any intermediate hops to a Dynomite coordinator node that is not the owner of the specified data.
+ Application specific local rack affinity based request routing to Dynomite nodes.
+ Application resilience by intelligently failing over to remote racks when local Dynomite rack nodes fail.
+ Application resilience against network glitches by constantly monitoring connection health and recycling unhealthy connections.
+ Capability of surgically routing traffic away from any nodes that need to be taken offline for maintenance.
+ Flexible retry policies such as exponential backoff etc
+ Insight into connection pool metrics
+ Highly configurable and pluggable connection pool components for implementing your advanced features.
 
