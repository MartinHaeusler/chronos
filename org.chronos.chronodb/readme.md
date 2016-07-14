<img src="https://github.com/MartinHaeusler/chronos/blob/master/readmeResources/chronoDBLogo.png" width="400">

About ChronoDB
==============

ChronoDB is one of the major components of [Chronos](https://github.com/MartinHaeusler/chronos). It is a key-value store with versioning and associated temporal features. As with all Chronos Components, ChronoDB is written in 100% pure Java and should run in any environment supported by JRE 1.8 or later.


Feature Highlights
==================

 - Key-Value Store with transparent system time versioning support
 - Full ACID (isolation level "serializable")
 - Easy-to-use API
 - Works on three backends: file (default), in-memory, and SQL (experimental)
 - Temporal secondary indexing
 - Fluent indexer query language
 - Lightweight branching support
 - Incremental commits for loading large batches of data
 - Equal performance on all timestamps (no performance penalty for queries on historical data)
 - Writing DB dumps into a configurable plain-text format (XML), and reading them again
 - Easy configuration via `*.properties` files or code
 - No background threads, no `static` magic, instantiate the database as often as needed within the same JVM
 - Thoroughly unit-tested
 - Full JavaDoc on the public API

Getting Started
===============

ChronoDB will very soon be available via maven. Until then, clone the repository and run `gradlew.build` in the project root directory.
*(The maven coordinates will be shown here as soon as it is available on the repository)*

ChronoDB employs an API design which we like to call a *Forward API*. It is designed to make the best possible use of code completion in an IDE, such as Eclipse, IntelliJ IDEA, Netbeans, or others. The key concept is to start with a simple object, and the rest of the API unfolds via code completion.

Let's create a new instance of ChronoDB. Following the Forward API principle, you start simple - with the `ChronoDB` interface. Code completion will reveal the static `FACTORY` field. From there, it's a fluent builder pattern:
   
```java
   ChronoDB db = ChronoDB.FACTORY.create().inMemoryDatabase().build();
```
    
With this new instance, we can create a transaction (ChronoDB is fully ACID compliant) and add some data:

```java
   ChronoDBTransaction tx = db.tx();
   tx.put("Hello", "World");         // writes to the default keyspace
   tx.put("Math", "Pi", 3.1415);     // writes to the "Math" keyspace
   tx.commit("My commit message");   // GIT-Style commit messages are supported (but are optional)
```



The mindful reader may have witnessed that the code above looks like a regular key-value store. There is nothing "temporal" about it. Indeed, the versioning in ChronoDB is fully transparent to the client programmer. Unless you explicitly **want** to deal with the temporal aspects yourself (e.g. for temporal queries), the ChronoDB API will take care of it for you.

Let's add some temporal querying to the mix. Let's say that our store has been existing for quite some time, and we want to query the state of the store at a specific date. As you can see, this is extremely simple with ChronoDB:

```java
   Date date = ...; // the date we want to query
   ChronoDBTransaction tx = db.tx(date.getTime());  // tx() without argument would refer to the 'head' revision
   // all queries we run on the transaction now work on the specified date
   tx.get("MyKey"); // will return the value associated with "MyKey" at the specified date
```

It is also easy to find out on what timestamp a query is operating, by calling `transaction.getTimestamp()`. Please note that, while a transaction is open, its associated timestamp can't change. To query different timestamps, simply use one transaction per timestamp.

Let's get even more involved with the temporal queries. What about finding out when a key changed?

```java
	ChronoDBTransaction tx = db.tx();
	// fetch the change timestamps
	Iterator<Long> timestamps = tx.history("MyKey");
	// iterate over the timestamps
	while(timestamps.hasNext()){
		long timestamp = timestamps.next();
		// open a transaction on this timestamp and fetch the value of MyKey
		Object value = db.tx(timestamp).get("MyKey");
		// do something with the value, e.g. display it on the UI, compare it to something...
		// Attention: 'value' may be NULL if the key was removed at this timestamp!
	}
```

Okay, but what about finding all modifications in a certain time range, regardless to which key? No problem:
```java
ChronoDBTransaction tx = chronoDB.tx();
Iterator<TemporalKey> modifiedKeys = tx.getModificationsInKeyspaceBetween("myKeyspace", 0L, tx.getTimestamp());
while(modifiedKeys.hasNext()){
	TemporalKey tKey = modifiedKeys.next();
	String key = tKey.getKey(); // the key that was modified
	long timestamp = tKey.getTimestamp(); // the commit timestamp at which the key was modified
	// remember that ChronoDB can store commit messages?
	String commitMessage = (String)tx.getCommitMetadata(timestamp);
	System.out.println(commitMessage);
}
```

The examples above are all fairly basic. ChronoDB can actually do a lot more. Starting from the `ChronoDB` and `ChronoDBTransaction` interfaces, ask your favorite IDE for code completion and explore the many features ChronoDB has to offer!

Frequently Asked Questions
==========================

### Is it stable?
The public API of ChronoDB is quite stable and not very likely to change drastically any time soon. However, the **persistence format is subject to change without prior notice**. ChronoDB is a research project after all.

### Which value types are supported?
Note that the `value` in `tx.put(key, value)` can be *any* Java object. The only constraint is that it must be (de-)serializable by the [Kryo Serializer](https://github.com/EsotericSoftware/kryo) which is employed internally. Usually, this means that:
 - An object is serializable if it has a default constructor, and no final fields, and...
 - ... only refers to other objects if they are serializable as well.

Any object that follows the Java Beans pattern is fair game here, as well as any primitive Java type (e.g. `int`, `float`, `double`...), primitive wrapper class (e.g. `Integer`, `Float`, ...), most collections (e.g. `java.util.HashSet`, `java.util.ArrayList`...) and many other classes in the JDK (including `String`, `java.util.Date`, and many more). However, we **strongly recommend** to keep it simple and stick to primitives and collections of primitives whenever possible to avoid trouble.

In general, `null` is **never a valid value**. When `tx.get("mykey")` returns `null`, it indicates that there is no value for the key.
