package com.redislabs.research.redis;

import com.redislabs.research.Document;
import com.redislabs.research.Index;
import com.redislabs.research.Query;
import com.redislabs.research.Spec;


import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.zip.CRC32;

/**
 * Created by dvirsky on 21/02/16.
 */
public class PartitionedIndex implements Index {

    Index[] partitions;

    public PartitionedIndex(String name, Spec spec, int numPartitions, String ...redisURIs ) {

        partitions = new Index[numPartitions];
        for (int i =0; i < numPartitions; i++) {
            String pname = String.format("%s{%d}", name, i);
            partitions[i] = new SimpleIndex(redisURIs[i % redisURIs.length], pname, spec);
        }

    }

    private CRC32 hash = new CRC32();
    synchronized int partitionFor(String id) {
        hash.reset();
        hash.update(id.getBytes());
        return (int) (hash.getValue() % partitions.length);
    }

    @Override
    public Boolean index(Document  ...docs) {

        // TODO: Make this transactional and pipelined
        for (Document doc : docs) {
            partitions[partitionFor(doc.id())].index(doc);
        }

        return true;
    }

    @Override
    public List<String> get(Query q) throws IOException {
        // TODO: parallelize
        List<List<String>> results = new ArrayList<>(partitions.length);

        for (Index idx : partitions) {
            results.add(idx.get(q));
        }

        List<String> ret = new ArrayList<>(q.sort.offset + q.sort.limit);
        for (List<String> tmp : results) {
            if (tmp != null && tmp.size() > 0) {
                ret.addAll(tmp);
            }
            if (ret.size() >= q.sort.offset+q.sort.limit) {
                break;
            }
        }
        return ret.subList(q.sort.offset, Math.min(q.sort.offset+q.sort.limit, ret.size()));

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
}
