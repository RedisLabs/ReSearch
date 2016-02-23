# ReSearch - Redis Scalable Search Engine

## Overview

Research is a library wrapping Redis with advanced search capabilities, 
automating indexing and retrieval.

Main Features include:

* Fast Auto-suggestion using lexical ranges.
* Multi-facet filters, including ranges and geo-filtering.
* Designed to scale: Indexes are partitioned across multiple redis instances, 
a design which allows scaling across large document sets and multi-machine clusters with ease.
* Simple API with a structured query builder.
 
## TL;DR - API Usage Example

Here's what you need to get things going fast:

```java

import com.redislabs.research.*;
import com.redislabs.research.redis.*;

```

### Create an index spec:

```java

// Create an index spec, telling ReSearch how to index documents:
// this spec tells the engine to create a prefix based index for a property named "title",
// indexing word-suffixes of it for completion as well 
Spec spec = new Spec(Spec.prefix("title", true));
```

### Create indexes and a document store

```java
// Create a PartitionedIndex which partitions indexes across multiple cluster nodes

// the number of internal partitions the index has. This should be higher than the number of
// cluster shards you have.
int numPartitions = 8;
// distributed query timeout
int timeoutMS = 500;
// redis hosts to connect to
String[] redisHosts = new String[] {"redis://localhost:6379", "redis://localhost:6380", ... };

Index idx = new PartitionedIndex("myIndex", //index name
        spec, 
        numPartitions, 
        timeout,
        redisHosts);


// Create a document store. The index only retrieves document ids. 
// The store actually holds the documents. This is optional if you want to implement a store yourself
// JSONStore just stores documents as JSON blobs in redis
DocumentStore st = new JSONStore("redis://localhost:6379");
```

### Index some documents

```java

Document[] docs = {
        new Document("doc1").set("title", "Redis in action")
            .set("description", "A book about redis in action"),
            
        new Document("doc2").set("title", "Redis in traction")
            .set("description", "A book about redis in traction")
};            

idx.index(docs); // this call accepts multiple documents and indexes them as a single transaction
```

### 5. query the index

```java

// Build a query
Query query = new Query("myIndex").filterPrefix("title", "redis");

// load the ids
List<String> ids = idx.get(query);

// load the documents
List<Document> docs = st.get(ids);
```

## Defining Indexes

## Creating Documents

## Retrieving Documents

## Design

## Index Design



