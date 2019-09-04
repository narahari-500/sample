package com.serverless;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.internal.StaticCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;

import org.apache.commons.io.IOUtils;
import org.archive.io.ArchiveRecord;
import org.archive.io.warc.WARCReader;
import org.codehaus.jackson.map.ObjectMapper;

import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

import javax.jms.*;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.zip.GZIPInputStream;

@SuppressWarnings("Duplicates")
public class CommonCrawl {
    private GZIPInputStream gzipInputStream = null;
    private BasicAWSCredentials awsCreds = new BasicAWSCredentials("AKIAIVJMBGBK4PRJ3SKA", "YTLxPha9rAUkPQzU/ywYD+urXjHSMWlnG1+n086T");
    private int counter = 0;
    AmazonS3 s3client = AmazonS3ClientBuilder.standard()
            .withRegion(Regions.US_EAST_1)
            .withCredentials(new StaticCredentialsProvider(awsCreds))
            .build();

    long count=0;

    private static Connection conn = null;

    public String handleRequest() throws JMSException, IOException, URISyntaxException {

        readWarcFileFromUrl("https://commoncrawl.s3.amazonaws.com/crawl-data/CC-MAIN-2019-26/segments/1560627997335.70/warc/CC-MAIN-20190615202724-20190615224509-00008.warc.gz", 12);
        return null;
    }


    public void readWarcFileFromUrl(String warcFileUrl, int warc_id) throws IOException, URISyntaxException, JMSException {

        long startTime = System.currentTimeMillis();

        AppupCounter appupCounter = new AppupCounter();
        // Warc Archiver should handle this.
        WARCReader warcreader = ArchiveReaderFactory.get(warcFileUrl);

        // Get the iterator, this iterator will have ArchiveRecord.
        Iterator<ArchiveRecord> archiveRecorder = warcreader.iterator();

        int iterations = 0;

        // Create producer
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
                List<String> hrefLinks = scrappers.linksExtractor();

                String metadata = scrappers.metadata();

                // Extract the domain from the URL.
                String domain = scrappers.domainExtractor(target);

                KeyValue2 keyValue2 = new KeyValue2();

                keyValue2.setEmail(mailto);
                keyValue2.setScriptUrls(scriptUrls);
                keyValue2.setUrls(target);
                keyValue2.setDomain(domain);
                keyValue2.setMetadata(metadata);
                keyValue2.setHrefLinks(hrefLinks);
                keyValueList.add(keyValue2);

                if (iterations >= 1000) {
                    OutputStream outputStream = new ByteArrayOutputStream();
                    ObjectMapper objectMapper = new ObjectMapper();
                    objectMapper.writeValue(outputStream, keyValueList);
                    persist(outputStream);
                    //appupCounter.recordUpdater(warc_id);
                    keyValueList.clear();
                    iterations = 0;
                   // System.exit(1);
                }
            } catch (Exception e) {
                System.out.println("Exception while processing the zip file " + e.getMessage());
            }
        } // While is closed here.

        //  System.out.format("We have completed warc file with script source count is %d and emails is %d",scriptCounter,emailsCounter);
        // System.out.println("");

        //   appupCounter.warcUrlUpdater(warc_id);
        //System.out.println("isProcessed Completed");

        long endTime = System.currentTimeMillis();
        System.out.println("That took " + (endTime - startTime) + " milliseconds");

    } // ReadWarchFile is closed.

    public String persist(OutputStream outputStream) throws SQLException, ParseException {
        conn = DBConn.getConnection();

       // conn.setAutoCommit(false);
        System.out.println("In persist method");

        try {
            String query = "insert into crawled_data(metadata,domain,urls,scriptUrls,hrefLinks,email) values(?,?,?,?,?,?)";

            PreparedStatement preparedStatement = conn.prepareStatement(query);

            JSONParser parser = new JSONParser();
            org.json.simple.JSONArray obj = (org.json.simple.JSONArray) parser.parse(String.valueOf(outputStream));

            Iterator iterator = obj.iterator();
            while (iterator.hasNext()) {
                org.json.simple.JSONObject jsonObject = (org.json.simple.JSONObject) iterator.next();

                preparedStatement.setString(1, (String) jsonObject.get("metadata"));
                preparedStatement.setString(2, (String) jsonObject.get("domain"));
                preparedStatement.setString(3, (String) jsonObject.get("urls"));
                preparedStatement.setString(4, (String) jsonObject.get("scriptUrls").toString());
                preparedStatement.setString(5, (String) jsonObject.get("hrefLinks").toString());
                preparedStatement.setString(6, (String) jsonObject.get("email").toString());
                preparedStatement.addBatch();
            }
            long startTime = System.currentTimeMillis();
            System.out.println("Persistence Started time is "+ startTime);
            int[] result = preparedStatement.executeBatch();
            long endTime = System.currentTimeMillis();
            count += endTime - startTime;

            System.out.println("The time taken for each persistence is " + (endTime - startTime) + " in milliseconds");
            System.out.println("1000 records are persisted");

            System.out.println("The total time taken for persistence is "+count + "In millis");

            System.out.println(result.toString());
            preparedStatement.clearBatch();

        } catch (
                Exception e) {
            e.printStackTrace();
        }

        return "Completed";
    }

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
    public static void main(String[] args) throws IOException, URISyntaxException {
        CommonCrawl commonCrawl = new CommonCrawl();
        try {
            commonCrawl.handleRequest();
        } catch (JMSException e) {
            e.printStackTrace();
        }
    }
}
