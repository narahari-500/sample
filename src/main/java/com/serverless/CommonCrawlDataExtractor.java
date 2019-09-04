package com.serverless;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.internal.StaticCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.SNSEvent;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.pool.PooledConnectionFactory;
import org.apache.commons.io.IOUtils;
import org.archive.io.ArchiveRecord;
import org.archive.io.warc.WARCReader;
import org.codehaus.jackson.map.ObjectMapper;
import org.json.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

import javax.jms.*;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.zip.GZIPInputStream;

@SuppressWarnings("Duplicates")
public class CommonCrawlDataExtractor implements RequestHandler<SNSEvent, String> {

    private static final String BUCKET_NAME = "finderio-common-crawler";
    private static final String COMMON_CRAWL_BUCKET = "commoncrawl";

    private static final String COMMON_CRAWL_WAT_TYPE = "wat";
    private static final String COMMON_CRAWL_WET_TYPE = "wet";
    private static final String COMMON_CRAWL_WARC_TYPE = "warc";

    private final static String WIRE_LEVEL_ENDPOINT
            = "ssl://b-593aeddf-9b0b-4d6b-9ac1-21b0f4c1d598-1.mq.us-east-1.amazonaws.com:61617?jms.useAsyncSend=true";
    private final static String ACTIVE_MQ_USERNAME = "appup";
    private final static String ACTIVE_MQ_PASSWORD = "Samba@123456";
    Destination destination = null;
    Session session = null;
    MessageProducer messageProducer = null;
    private GZIPInputStream gzipInputStream = null;
    private BasicAWSCredentials awsCreds = new BasicAWSCredentials("AKIAIVJMBGBK4PRJ3SKA", "YTLxPha9rAUkPQzU/ywYD+urXjHSMWlnG1+n086T");
    private int counter = 0;
    AmazonS3 s3client = AmazonS3ClientBuilder.standard()
            .withRegion(Regions.US_EAST_1)
            .withCredentials(new StaticCredentialsProvider(awsCreds))
            .build();

    Connection connection = null;

    public String handleRequest(SNSEvent snsEvent, Context context) {

        String record = snsEvent.getRecords().get(0).getSNS().getMessage();
        org.json.simple.JSONObject jsonObject = null;

        try {
            jsonObject = (org.json.simple.JSONObject) new JSONParser().parse(record);
        } catch (ParseException e) {
            e.printStackTrace();
        }

        String warc_url = (String) jsonObject.get("warc_url");
        int warc_id = Integer.parseInt((String)jsonObject.get("warc_id"));

        final ActiveMQConnectionFactory connectionFactory =
                new ActiveMQConnectionFactory(WIRE_LEVEL_ENDPOINT);
        connectionFactory.setUserName(ACTIVE_MQ_USERNAME);
        connectionFactory.setPassword(ACTIVE_MQ_PASSWORD);
        connectionFactory.setUseAsyncSend(Boolean.TRUE);

        //PooledConnectionFactory pooledConnectionFactory = new PooledConnectionFactory();

        //pooledConnectionFactory.setMaxConnections(50);
        //pooledConnectionFactory.setConnectionFactory(pooledConnectionFactory);

        try {
            connection = connectionFactory.createConnection();
            session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        } catch (JMSException e) {
            e.printStackTrace();
        }

        String line = "https://commoncrawl.s3.amazonaws.com/" + warc_url;

        System.out.println("For warc Url "+line);
        try {
            readWarcFileFromUrl(line,warc_id);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (URISyntaxException e) {
            e.printStackTrace();
        } catch (JMSException e) {
            e.printStackTrace();
        }

        return null;
    }

    public MessageProducer createProducer(String queueName) throws JMSException {
        if (messageProducer == null) {
            destination = session.createQueue(queueName);
            messageProducer = session.createProducer(destination);
            messageProducer.setDeliveryMode(DeliveryMode.PERSISTENT);
        }
        return messageProducer;
    }

    // Create ActiveMQ connection factory, this method is used by createPooledConnectionFactory.
    private static ActiveMQConnectionFactory createActiveMQConnectionFactory() {
        // Create a connection factory.
        final ActiveMQConnectionFactory connectionFactory =
                new ActiveMQConnectionFactory(WIRE_LEVEL_ENDPOINT);

        // Pass the username and password.
        connectionFactory.setUserName(ACTIVE_MQ_USERNAME);
        connectionFactory.setPassword(ACTIVE_MQ_PASSWORD);
        connectionFactory.setUseAsyncSend(Boolean.TRUE);
        return connectionFactory;
    }

    public void readWarcFileFromUrl(String warcFileUrl,int warc_id) throws IOException, URISyntaxException, JMSException {

        long startTime = System.currentTimeMillis();


        AppupCounter appupCounter = new AppupCounter();
        // Warc Archiver should handle this.
        WARCReader warcreader = ArchiveReaderFactory.get(warcFileUrl);

        // Get the iterator, this iterator will have ArchiveRecord.
        Iterator<ArchiveRecord> archiveRecorder = warcreader.iterator();

        int iterations = 0;

        // Create producer
        messageProducer = createProducer("finderio");

        // Iterate through each record.
        List keyValueList = new ArrayList<>();

        while (archiveRecorder.hasNext()) {
            try {

                String type = "warc";
                ArchiveRecord archiveRecord = archiveRecorder.next();

                final String target = archiveRecord.getHeader().getUrl();
                if (target == null)
                    continue;

                String body = null;
                if (type.equalsIgnoreCase("warc")) {
                    body = getBodyFromWARC(archiveRecord);
                    iterations++;
                }
                if (body == null) {
                    iterations--;
                    continue;
                }
                Document doc = Jsoup.parse(body);
                Scrappers scrappers = new Scrappers(doc);

                // Extract emails.
                List<String> mailto = scrappers.emailsExtractor();
                List<String> scriptUrls = scrappers.scriptUrlsExtractor();

                // Extract the domain from the URL.
                String domain = scrappers.domainExtractor(target);

                KeyValue2 keyValue2 = new KeyValue2();

                keyValue2.setEmail(mailto);
                keyValue2.setScriptUrls(scriptUrls);
                keyValue2.setUrls(target);
                keyValue2.setDomain(domain);
                keyValueList.add(keyValue2);

                if (iterations > 10000) {
                    OutputStream outputStream = new ByteArrayOutputStream();
                    ObjectMapper objectMapper = new ObjectMapper();
                    objectMapper.writeValue(outputStream, keyValueList);
                    OutputStream text = outputStream;
                    TextMessage textMessage = session.createTextMessage(String.valueOf(text));
                    messageProducer.send(textMessage);
                    System.out.println("Message sent to active mq");
                    appupCounter.recordUpdater(warc_id);
                    System.out.println("Updated the record updater");
                    keyValueList.clear();
                    iterations = 0;
                    //System.exit(1);
                }
            } catch (Exception e) {
                System.out.println("Exception while processing the zip file " + e.getMessage());
            }
        } // While is closed here.

        //  System.out.format("We have completed warc file with script source count is %d and emails is %d",scriptCounter,emailsCounter);
        // System.out.println("");

        appupCounter.warcUrlUpdater(warc_id);
        System.out.println("isProcessed Completed");

        long endTime = System.currentTimeMillis();
        System.out.println("That took " + (endTime - startTime) + " milliseconds");

    } // ReadWarchFile is closed.

    /**
     * Method will extract the warc file body content from the archive record
     *
     * @param archiveRecord
     * @return
     * @throws IOException
     */
    public String getBodyFromWARC(ArchiveRecord archiveRecord) {
        try {
            if (archiveRecord.getHeader().getMimetype().equals("application/http; msgtype=response")) {
                byte[] rawData = IOUtils.toByteArray(archiveRecord, archiveRecord.available());
                String content = new String(rawData);
                String headerText = content.substring(0, content.indexOf("\r\n\r\n"));
                if (headerText.contains("Content-Type: text/html")) {
                    String body = content.substring(content.indexOf("\r\n\r\n") + 4);
                    return body;
                }
            }
        } catch (IOException ioe) {
            System.out.println("Exception while parsing ArchiveRecord " + ioe.getMessage());
        }
        return "";
    }
//    public static void main(String[] args) throws IOException, URISyntaxException {
//        CommonCrawlDataExtractor commonCrawlDataExtractor = new CommonCrawlDataExtractor();
//        try {
//            commonCrawlDataExtractor.handleRequest();
//        } catch (JMSException e) {
//            e.printStackTrace();
//        }
//    }
}
