# SeaStar
Lightweight in-memory CqlSession driver for testing, essentially a mock for Cassandra.

## Goals (in order of precedence)
1. Maintain a robust test suite ensuring that functionality of a real cassandra database is mirrored correctly. If a query fails live it should fail in a similar fashion in seastar.
2. Minimize start up time to act as a viable alternative for [TestContainers](https://java.testcontainers.org/modules/databases/cassandra/)
3. All code should be thread-safe.

## Future Improvements / TODO
* SeaStar, as written, necessarily deserializes and re-serializes data. This is completely unnecessary. I was simply too lazy to implement/override all the default getters.
* Perform all operations in a single thread to avoid concurrency issues
* Audit all locks
* Implement SchemaChangeListener
* Static column support
* Service locator for query processors

### What's with the name?
It's a bad pun. Cassandra is often abbreviated to C*, so I called this library SeaStar.

## Example
```java
// TODO
```
