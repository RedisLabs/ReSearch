package com.redislabs.research.redis;

import com.redislabs.research.Document;
import com.redislabs.research.Index;
import com.redislabs.research.Query;
import com.redislabs.research.Spec;
import com.sun.deploy.util.ArrayUtil;


import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.CRC32;

/**
 * PartitionedIndex wraps multiple partitions of simple indexes and queries them concurrently
 */
public class PartitionedIndex implements Index {

    Index[] partitions;

    // this is basically java 8's newWorkStealingPool
    static ExecutorService pool = new ForkJoinPool(Runtime.getRuntime().availableProcessors(),
            ForkJoinPool.defaultForkJoinWorkerThreadFactory,
            null, true);

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
        final PriorityQueue<Entry> entries = new PriorityQueue<>(q.sort.limit*partitions.length);

        List<Callable<List<Entry>>> tasks = new ArrayList<>(partitions.length);

        // submit the sub tasks to the thread pool
        for (final Index idx : partitions) {

            tasks.add(new Callable<List<Entry>>() {
                @Override
                public List<Entry> call() throws Exception {
                    return idx.get(q);
                }
            });
        }

        List<Future<List<Entry>>> futures = pool.invokeAll(tasks, timeoutMilli, TimeUnit.MILLISECONDS);

        for (Future<List<Entry>> future : futures ) {
            if (!future.isCancelled()) {
                try {
                    List<Entry> es = future.get();
                    if (es != null) {
                        entries.addAll(es);
                    }
                } catch (ExecutionException e) {
                    e.printStackTrace();
                }
            }
        }


        while (entries.size() > q.sort.limit) {
            entries.poll();
        }

        List<Entry> ret = new ArrayList<>(entries.size());

        while (entries.size() > 0) {
            ret.add(entries.poll());
        }
        Collections.reverse(ret);

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
