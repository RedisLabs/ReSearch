package com.redislabs.research.redis;

import com.redislabs.research.Document;
import com.redislabs.research.Index;
import com.redislabs.research.Query;
import com.redislabs.research.Spec;


import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.concurrent.*;
import java.util.zip.CRC32;

/**
 * PartitionedIndex wraps multiple partitions of simple indexes and queries them concurrently
 */
public class PartitionedIndex implements Index {

    Index[] partitions;
    ExecutorService pool;
    int timeoutMilli;
    String name;

    public static PartitionedIndex newSimple(String name, Spec spec, int numPartitions, int timeoutMilli,
                          int numThreads,
                          String ...redisURIs) throws IOException {
        return new PartitionedIndex(new SimpleIndex.Factory(), name,spec,numPartitions,timeoutMilli,numThreads,redisURIs);
    }

    public static PartitionedIndex newFulltext(String name, Spec spec, int numPartitions, int timeoutMilli,
                                             int numThreads,
                                             String ...redisURIs) throws IOException {
        return new PartitionedIndex(new FullTextFacetedIndex.Factory(), name,spec,numPartitions,timeoutMilli,numThreads,redisURIs);
    }


    public PartitionedIndex(IndexFactory factory, String name, Spec spec, int numPartitions, int timeoutMilli,
                            int numThreads,
                            String ...redisURIs ) throws IOException {
        this.name = name;
        partitions = new Index[numPartitions];
        this.timeoutMilli = timeoutMilli;
        for (int i =0; i < numPartitions; i++) {
            String pname = String.format("%s{%d}", name, i);
            partitions[i] = factory.create(pname, spec, redisURIs[i % redisURIs.length]);
        }

        pool = Executors.newFixedThreadPool(numThreads);


    }

    private CRC32 hash = new CRC32();
    synchronized int partitionFor(String id) {
        hash.reset();
        hash.update(id.getBytes());
        return (int) (hash.getValue() % partitions.length);
    }

    @Override
    public Boolean index(Document  ...docs) throws IOException {

        ArrayList[] parts = new ArrayList[partitions.length];
        for (int i =0; i < partitions.length; i++) {
            parts[i] = new ArrayList(1);
        }
        // TODO: Make this transactional and pipelined
        for (Document doc : docs) {
            parts[partitionFor(doc.getId())].add(doc);

        }

        for (int i =0; i < partitions.length; i++) {
            if (parts[i].size() > 0) {
                partitions[i].index((Document[]) parts[i].toArray(new Document[parts[i].size()]));
            }
        }

        return true;
    }

    @Override
    public List<Entry> get(final Query q) throws IOException, InterruptedException {

        // this is the queue we use to aggregate the results
        final BlockingDeque<List<Entry>> queue = new LinkedBlockingDeque<>(partitions.length);

        // submit the sub tasks to the thread pool
        for (Index idx : partitions) {
            final Index fidx = idx;

            pool.submit( new Callable<Void>() {
                public Void call() throws IOException, InterruptedException {
                    List<Entry> r = fidx.get(q);

                    queue.add(r);
                    return null;
                }
            });

        }


        // collect the results
        PriorityQueue<Entry> entries = new PriorityQueue<Entry>(q.sort.limit, new Comparator<Entry>(){

            @Override
            public int compare(Entry e1, Entry e2) {
                return e1.score == e2.score ? 0 : (e1.score > e2.score ? -1 : 1);
            }
        });

        int took = 0;
        while (took < partitions.length) {

            List<Entry> res = queue.poll(timeoutMilli, TimeUnit.MILLISECONDS);
            if (res != null) {
                entries.addAll(res);
                took++;
            }

        }

        List<Entry> ret = new ArrayList<>(entries.size());
        while (entries.size() > 0) {
            ret.add(entries.poll());
        }
        return ret;



    }

    @Override
    public Boolean delete(String... ids) {

        for (Index idx : partitions) {
            idx.delete(ids);
        }

        return true;
    }

    @Override
    public Boolean drop() {

        for (Index idx : partitions) {
            idx.drop();
        }

        return true;

    }

    @Override
    public String id() {
        return name;
    }
}
