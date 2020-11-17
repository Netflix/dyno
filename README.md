# Dyno 
[![Build Status](https://travis-ci.org/Netflix/dyno.svg)](http://travis-ci.org/Netflix/dyno)
[![Dev chat at https://gitter.im/Netflix/dynomite](https://badges.gitter.im/Netflix/dynomite.svg)](https://gitter.im/Netflix/dynomite?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)
[![Apache V2 License](http://img.shields.io/badge/license-Apache%20V2-blue.svg)](https://github.com/Netflix/dyno/blob/master/LICENSE)

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
 
## Build

Dyno comes with a Gradle wrapper.

```bash
git clone https://github.com/Netflix/dyno.git

cd dyno

./gradlew clean build
```

The `gradlew` script will download all dependencies automatically and then build Dyno.

## Contributing

Thank you for your interest in contributing to the Dyno project. Please see the [Contributing](./CONTRIBUTING.md) file for instructions on how to submit a pull request.

> Tip: It is always a good idea to submit an issue to discuss a proposed feature before writing code.

## Help

Need some help with either getting up and going or some problems with the code?

- [Submit an issue](https://github.com/Netflix/dyno/issues)
- Chat with us on [![Dev chat at https://gitter.im/Netflix/dynomite](https://badges.gitter.im/Netflix/dynomite.svg)](https://gitter.im/Netflix/dynomite?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)


## License

Licensed under the [Apache License, Version 2.0](./LICENSE)
