package uk.me.candle.examples.camel.ddbstream;

import java.net.URI;
import java.util.Map;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBStreams;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBStreamsClient;
import com.amazonaws.services.dynamodbv2.model.Record;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import ch.qos.logback.classic.Level;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ShardIteratorType;
import org.apache.camel.Body;
import org.apache.camel.CamelContext;
import org.apache.camel.Endpoint;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.impl.SimpleRegistry;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main {
    private static final Logger LOG = LoggerFactory.getLogger(Main.class);

    private static class Args {
        @Option(name = "--region", required = true, usage = "The AWS region to use")
        private String region;

        @Option(name = "--sequence-number", usage = "The sequence number to start to start producing exchanges at. Required if --iterator-type is one of {at,after}_sequence_number")
        private String sequenceNumber;

        @Option(name = "--iterator-type", usage = "One of: trim_horizon, latest, at_sequence_number or after_sequence_number")
        private String iteratorType;

        @Option(name = "--table-name", required = true, usage = "DynamoDB table from which to obtain the change stream")
        private String tableName;

        public Region getRegion() {
            return Region.getRegion(Regions.fromName(region));
        }

        public String getSequenceNumber() {
            return sequenceNumber;
        }

        public ShardIteratorType getIteratorType() {
            return ShardIteratorType.fromValue(iteratorType.toUpperCase());
        }

        public String getTableName() {
            return tableName;
        }
    }

    public static class Proc {
        private static final Logger LOG = LoggerFactory.getLogger(Proc.class);
        public void handle(@Body Record body) {

            LOG.info("Event Type: {}", body.getEventName());
            LOG.info("Sequence Number: {}", body.getDynamodb().getSequenceNumber());
            Map<String, AttributeValue> keysMap = body.getDynamodb().getKeys();
            final Map<String, AttributeValue> oldImage = body.getDynamodb().getOldImage();
            final Map<String, AttributeValue> newImage = body.getDynamodb().getNewImage();

            logMap(keysMap, "key");
            logMap(newImage, "new");
            logMap(oldImage, "old");

            Map<String, AttributeValue> added = new HashMap<>(newImage == null ? Collections.emptyMap() : newImage);
            if (oldImage != null) oldImage.keySet().forEach(k -> added.remove(k));

            Map<String, AttributeValue> removed = new HashMap<>(oldImage == null ? Collections.emptyMap() : oldImage);
            if (newImage != null) newImage.keySet().forEach(k -> removed.remove(k));

            Map<String, Pair<AttributeValue, AttributeValue>> modified = new HashMap<>();
            if (newImage != null) {
                newImage.forEach((k, v) -> {
                    Map<String, AttributeValue> old = oldImage;
                    if (old != null && old.containsKey(k) && !old.get(k).equals(v)) {
                        modified.put(k, new Pair<>(old.get(k), v));
                    }
                });
            }

            if (!added.isEmpty()) logMap(added, "added");
            if (!removed.isEmpty()) logMap(removed, "removed");
            if (!modified.isEmpty()) {
                int max = modified.keySet().stream().map(String::length).reduce(0, (a, b) -> Math.max(a, b));
                modified.forEach((k, v) -> LOG.info("modified: {} -> {} ==> {}", padr(k, max+2, ' '), v.a, v.b));
            }
        }

        private void logMap(Map<String, AttributeValue> newRecord, String pfx) {
            if (newRecord == null) return;
            int max = newRecord.keySet().stream().map(String::length).reduce(0, (a, b) -> Math.max(a, b));
            newRecord.forEach((k, v) -> LOG.info("{}: {} -> {}", pfx, padr(k, max+2, ' '), v.toString()));
        }

    }

    private static class Pair<A, B> {
        private final A a;
        private final B b;

        Pair(A a, B b) {
            this.a = a;
            this.b = b;
        }
    }

    private static CharSequence padr(String in, int toLength, char padChar) {
        if (in.length() >= toLength) return in;
        char[] arr = new char[toLength];
        Arrays.fill(arr, padChar);
        System.arraycopy(in.toCharArray(), 0, arr, 0, in.length());
        return new String(arr);
    }

    private static class Routes extends RouteBuilder {
        private final Endpoint in;

        private Routes(Endpoint in) {
            this.in = in;
        }

        @Override
        public void configure() throws Exception {
            from(in)
                    .routeId("main_route")
                    .log("Recieved exchange")
                    .bean(new Proc());
        }
    }

    public static void main(String[] args) throws Exception {
        Args argsBean = new Args();
        CmdLineParser parser = new CmdLineParser(argsBean);
        try {
            parser.parseArgument(args);
            new Main(argsBean).run();
        } catch (CmdLineException e) {
            System.out.println(e.getMessage());
            parser.printSingleLineUsage(System.out);
            System.out.println("");
            System.out.println("");
            parser.printUsage(System.out);
            System.out.println("");
        }
    }

    private final Args args;

    Main(Args args) {
        this.args = args;
    }

    void run() throws Exception {
        reConfigureLogging();

        ScheduledExecutorService executor = Executors.newScheduledThreadPool(2);

        ClientConfiguration conf = new ClientConfiguration();
        applyProxy(conf);

        SimpleRegistry registry = new SimpleRegistry();
        AmazonDynamoDBStreams client = args.getRegion().createClient(AmazonDynamoDBStreamsClient.class, null, conf);
        registry.put("ddbStreamsClient", client);

        CamelContext camelContext = new DefaultCamelContext(registry);
        camelContext.setTypeConverterStatisticsEnabled(true);
        Endpoint stream = camelContext.getEndpoint(
                  "aws-ddbstream://" + args.getTableName()
                + "?sequenceNumberProvider=" + args.getSequenceNumber()
                + "&iteratorType=" + args.getIteratorType()
                + "&amazonDynamoDbStreamsClient=#ddbStreamsClient"
                + "&maxResultsPerRequest=100"
                + "&scheduler.delay=500"
                );

        camelContext.addRoutes(new Routes(stream));
        camelContext.start();

        executor.schedule(() -> LOG.info("Type Converter stats: {}", camelContext.getTypeConverterRegistry().getStatistics()), 30, TimeUnit.SECONDS);

        Thread.sleep(TimeUnit.MINUTES.toMillis(10)); // lazy delay.
        executor.shutdownNow();
        camelContext.stop();
        LOG.info("Terminating after 10 minutes");
    }

    private static void reConfigureLogging() {
        System.getProperties().forEach(
                (k, v) -> {
                    String name = (String)k;
                    if (name.startsWith("LOG.")) {
                       ((ch.qos.logback.classic.Logger)LoggerFactory.getLogger(name.substring(4))).setLevel(Level.toLevel(v.toString()));
                    }
                }
        );
    }

    private static final List<String> PROXY_PROPS = new ArrayList<>();
    static {
        PROXY_PROPS.add("HTTPS_PROXY");
        PROXY_PROPS.add("https_proxy");
    }
    private static void applyProxy(ClientConfiguration conf) {
        AtomicReference<String> proxyString = new AtomicReference<>();
        PROXY_PROPS.stream()
                .map(s -> System.getenv(s))
                .filter(s -> s != null)
                .forEach(s -> proxyString.set(s));
        PROXY_PROPS.stream()
                .map(s -> System.getProperty(s))
                .filter(s -> s != null)
                .forEach(s -> proxyString.set(s));
        LOG.info("attempting to locate a proxy from {}", proxyString.get());
        if (proxyString.get() != null && !proxyString.get().isEmpty()) {
            URI proxy = URI.create(proxyString.get());
            conf.setProxyHost(proxy.getHost());
            conf.setProxyPort(proxy.getPort());
            LOG.info("Set proxy to: {}", proxy);
        }
    }
}
