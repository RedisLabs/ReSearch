package com.redislabs.research;

import com.google.gson.Gson;
import com.redislabs.research.redis.JSONStore;
import com.redislabs.research.redis.PartitionedIndex;
import org.apache.commons.cli.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;


/**
 * Created by dvirsky on 21/02/16.
 */
public class Main {

    public static class GeoJsonEntity {
        public static class Geometry  {
            public float[] coordinates;
            public String type;
        }

        public static class Classifier {
            public String category;
            public String subcategory;
            public String type;
        }

        public static class Properties {
            public String address;
            public String city;
            public Classifier[] classifiers;

            public String country;
            public String href;
            public String name;
            public String owner;
            public String phone;
            public String postcode;
            public String province;
            public String[] tags;
        }

        public Geometry geometry;
        public String id;
        public Properties properties;
        public String type;
    }


    /**
     * Encapsulation of command line options
     */
    static class RunOptions {
        String fileName;
        String []redisHosts;
        int numPartitions;
        boolean loadData;



        public RunOptions(String []args) {

            Options opts = new Options();
            opts.addOption(new Option("l", "load", false, "Only load data if set to true"));
            opts.addOption(new Option("f", "file", true, "input file to load"));
            opts.addOption(new Option("h", "help",  false, "print this message"));
            opts.addOption(new Option("p", "partitions",  true, "number of index partitions"));

            Option redisHosts = Option.builder("r")
                    .longOpt("redisHosts")
                    .hasArgs()
                    .argName("localhost:6379,...")
                    .desc("redis hosts, comma separated")
                    .build();
            opts.addOption(redisHosts);


            CommandLineParser parser = new DefaultParser();
            CommandLine cmd;
            try {
                cmd = parser.parse( opts, args);

                if (cmd.hasOption("help")) {

                    HelpFormatter formatter = new HelpFormatter();
                    formatter.printHelp( "java -jar research.jar", opts );

                    System.exit(0);
                }
            } catch (ParseException e) {
                System.err.println(e.toString());
                return;
            }

            this.fileName = cmd.getOptionValue("file");
            this.loadData = cmd.hasOption("load");
            this.numPartitions = Integer.parseInt(cmd.getOptionValue("partitions", "8"));


            String[] hosts = cmd.getOptionValue("redisHosts", "localhost:6379").split(",");
            this.redisHosts = new String[hosts.length];
            int i = 0;
            for (String h : hosts) {
                this.redisHosts[i++] = String.format("redis://%s", h);
            }

            System.out.println(Arrays.toString(this.redisHosts));





        }
    }


    static void loadData(String fileName, Index idx, DocumentStore store) throws IOException {


        Path path = FileSystems.getDefault().getPath("", fileName);
        BufferedReader rd =
                Files.newBufferedReader(path, Charset.forName("utf-8"));

        String line = null;
        Gson gson = new Gson();
        int  i =0;
        while ((line = rd.readLine()) != null) {

            GeoJsonEntity ent = gson.fromJson(line, GeoJsonEntity.class);

            Document doc = new Document(ent.id);
            doc.set("name", ent.properties.name);
            doc.set("city", ent.properties.city);
            doc.set("latlon", ent.geometry.coordinates);

            idx.index(doc);
            store.store(doc);

            i++;
            if (i % 10000 == 0) {
                System.out.println(i);
            }

        }


    }

    public static void benchmark(int numThreads, final int numTests, final Index idx) {

        final String[] prefixes = new String[]{"a", "b", "c", "ab", "ac", "ca", "do", "ma", "foo", "bar", "ax"};

        ExecutorService pool = Executors.newFixedThreadPool(numThreads);

        final AtomicInteger ctr = new AtomicInteger(0);
        final long startTime = System.currentTimeMillis();
        for (int i =0; i < numThreads; i++) {
            
            pool.submit(new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                    for (int x =0; x < numTests; x++) {

                        idx.get(new Query("locs_name").filterPrefix("name", prefixes[x%prefixes.length]));
                        int total = ctr.incrementAndGet();
                        if (total % 10000 == 0) {

                            long now = System.currentTimeMillis();
                            System.out.println((double)total/((double)(now-startTime)/1000F));

                        }
                    }
                    return null;
                }
            });
            
        }

    }


    public static void main(String [] args) throws IOException {

        RunOptions opts = new RunOptions(args);

        Spec spec = new Spec(Spec.prefix("name", false));


        PartitionedIndex pi = new PartitionedIndex("locs_name", spec, opts.numPartitions, 500,
                opts.redisHosts);
        DocumentStore st = new JSONStore(opts.redisHosts[0]);

        if (opts.loadData && !opts.fileName.isEmpty()) {
            System.out.println("Loading data from " + opts.fileName);
            pi.drop();
            loadData(opts.fileName, pi, st);
        }

       benchmark(4, 1000000, pi);

        //pi.drop();
        //loadData(args[0], pi, st);
    }
}
