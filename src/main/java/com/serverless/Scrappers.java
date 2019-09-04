package com.serverless;

import org.apache.log4j.Logger;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

@SuppressWarnings("Duplicates")
public class Scrappers {

    static final Logger log = Logger.getLogger(Scrappers.class);

    Document document = null;

    public Scrappers(Document document){
        this.document = document;
    }

    public List<String> emailsExtractor() {

      //  log.info("Extracting emails from payload");

        String[] mails = {};

        String hrefLinks = "";

        String email = "";

        Elements emailLinks = null;

        List<String> mailto = new ArrayList<>();
        try {

            emailLinks = this.document.select("a[href^=mailto:]");

            for (Element emails : emailLinks) {
                hrefLinks = emails.attr("href");
                mails = hrefLinks.split(":");
                email = mails[1];
                try {
                    if (email == ":") {
                        email = "";
                    }
                } catch (ArrayIndexOutOfBoundsException aiob) {
                  // System.out.println(aiob);
                }
                mailto.add(String.valueOf(email));
            }
        } catch (ArrayIndexOutOfBoundsException e) {
          // System.out.println(e);
        }
        return mailto;

    }

    public String domainExtractor(String target) throws URISyntaxException {

        //log.info("Extracting domains");

        String domainName = "";
        String domain = "";
        URI uri = new URI(target);
        try {
            domain = uri.getHost();
            domainName = domain.startsWith("www.") ? domain.substring(4) : domain;
        } catch (Exception e) {
            //System.out.println(e);
        }
        return domainName;
    }

    public List<String> scriptUrlsExtractor() {

       // log.info("Extracting script source urls from the payload");

        List<String> scriptUrls = new ArrayList<>();

        try {
            Elements scripts = this.document.select("script[src]");

            for (Element script : scripts) {
                scriptUrls.add(script.attr("src"));
            }
        } catch (Exception e) {
         //   System.out.println(e);
        }
        return scriptUrls;
    }

    public List<String> linksExtractor() {

     // log.info("Extracting Links from payload");

        List<String> links = new ArrayList<>();

        try {


            Elements link = this.document.select("a[href]");

            for (Element eachLinks : link) {
                links.add(eachLinks.attr("href"));
            }
        } catch (Exception e) {
         //   System.out.println(e);
        }

        return links;
    }

    public String metadata() {
        String metadata = "";
        try {
            metadata =
                    this.document.select("meta[name=description]").get(0)
                            .attr("content");

        } catch (Exception ex) {
           //log.info(ex);
        }
        return metadata;
    }
}
