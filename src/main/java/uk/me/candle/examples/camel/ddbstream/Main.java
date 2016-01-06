package uk.me.candle.examples.camel.ddbstream;

import java.net.URI;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBStreams;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBStreamsClient;
import com.amazonaws.services.dynamodbv2.model.Record;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import ch.qos.logback.classic.Level;
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
            LOG.info("record: {}", body);
        }
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

    private static void applyProxy(ClientConfiguration conf) {
        String lowerCase = System.getProperty("https_proxy");
        String upperCase = System.getProperty("HTTPS_PROXY");
        String proxyString = lowerCase == null ? upperCase : lowerCase;
        LOG.info("attempting to locate a proxy from either {} or {}", lowerCase, upperCase);
        if (proxyString != null) {
            URI proxy = URI.create(proxyString);
            conf.setProxyHost(proxy.getHost());
            conf.setProxyPort(proxy.getPort());
            LOG.info("Set proxy to: {}", proxy);
        }
    }
}
