package com.redislabs.research;

import org.apache.commons.cli.*;

import java.io.OutputStream;
import java.io.PrintStream;
import java.lang.ref.PhantomReference;
import java.lang.ref.Reference;
import java.lang.ref.WeakReference;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by dvirsky on 10/03/16.
 */
public abstract class Benchmark {
    public int numThreads;
    public long runDuration;
    public String tag;
    public int numTests;

    public static class Flags {


        protected Options opts;
        protected CommandLine cmd;


        public Flags(Option ...extraOptions) {

            opts = new Options();

            opts.addOption(new Option("h", "help", false, "print this message"));
            opts.addOption(new Option("T", "tag", true, "tag this benchmark for file output (optional)"));

            opts.addOption(Option.builder("d")
                    .longOpt("duration")
                    .hasArgs()
                    .type(Long.class)
                    .argName("seconds")
                    .desc("Benchmark run duration, in seconds")
                    .build());

            opts.addOption(Option.builder("t")
                    .longOpt("threads")
                    .hasArgs()
                    .argName("num")
                    .type(Integer.class)
                    .desc("Benchmark run duration, in seconds")
                    .build());

            for (Option opt : extraOptions) {
                opts.addOption(opt);
            }

        }


        public void parse(String []args) {

            CommandLineParser parser = new DefaultParser();

            try {
                cmd = parser.parse(opts, args);

                if (cmd.hasOption("help")) {

                    HelpFormatter formatter = new HelpFormatter();
                    formatter.printHelp( "java -jar research.jar", opts );

                    System.exit(0);
                }
            } catch (ParseException e) {
                System.err.println(e.toString());

            }




        }
    }



    public interface Context {
        boolean tick();
    }

    private class ParallelContext implements Context {

        AtomicLong numTicks;
        AtomicLong lastCheckpointTime;
        AtomicLong lastCheckpointTicks;

        AtomicBoolean isRunning;

        private AtomicLong startTime ;
        private final long durationMS;
        private final long checkInterval;
        private final String name;
        private long endTime;


        public ParallelContext(String benchmarkName, long durationMS, long checkInterval) {

            this.durationMS = durationMS;
            name = benchmarkName;
            this.checkInterval = checkInterval;
            startTime = new AtomicLong(System.currentTimeMillis());
            numTicks = new AtomicLong(0);
            lastCheckpointTime = new AtomicLong(0);
            lastCheckpointTicks = new AtomicLong(0);
            isRunning = new AtomicBoolean(true);
        }

        private double rate(long ticks, long duration) {
            return (double)ticks/(duration/1000d);
        }
        private void printStats(long ticks, long now, PrintStream out) {
            Timestamp ts = new Timestamp(now);
            out.printf("%s> %s: %d iterations, current rate: %.02fops/sec, avg. rate: %.02fops/sec\n",
                    ts.toString(), name, ticks,
                    rate(ticks-lastCheckpointTicks.get(), now-lastCheckpointTime.get()),
                    rate(ticks, now-startTime.get())
            );
        }

        private boolean onCheckpoint(long ticks) {
            long now = System.currentTimeMillis();

            // if too long a time has passed - stop
            if (startTime.get() + durationMS <= now) {
                endTime = now;
                isRunning.set(false);
                System.out.println("Looks like we're done!");
                return false;
            }
            printStats(ticks, now, System.out);
            lastCheckpointTime.set(now);
            lastCheckpointTicks.set(ticks);
            return true;
        }

        public boolean tick() {

            long ticks = this.numTicks.incrementAndGet();

            if (ticks == 1) {
                startTime.set(System.currentTimeMillis());
                lastCheckpointTime.set(startTime.get());
            }
            if (ticks % checkInterval == 0) {
                return onCheckpoint(ticks);
            }

            return isRunning.get();
        }


    }


    protected Flags flags;

    public Benchmark(String[] argv, Option ...extraOptions) {

        flags = new Flags(extraOptions);
        flags.parse(argv);

        this.numThreads = Integer.parseInt(getOption("threads", "1"));
        this.runDuration = Integer.parseInt(getOption("duration", "60"));
        this.tag = getOption("tag", "");
    }

    public abstract void run(Context ctx);


    protected String getOption(String name, String defaultValue) {
        return flags.cmd.getOptionValue(name, defaultValue);
    }
    protected String getOption(String name) {
        return flags.cmd.getOptionValue(name);
    }

    protected boolean isOptionSet(String name) {
        return flags.cmd.hasOption(name);
    }
    public void start() {

        System.out.printf("Benchmarking %s using %d threads\n", tag, numThreads);
        ExecutorService pool = Executors.newFixedThreadPool(numThreads);


        final ParallelContext ctx = new ParallelContext(tag, runDuration*1000, 10000);
        for (int i =0; i < numThreads; i++) {

            pool.submit(new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                    try {
                        Benchmark.this.run(ctx);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    return null;
                }
            });

        }


        try {
            pool.awaitTermination(runDuration+5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        System.out.println("Benchmark finished!");




    }
}
