package com.serverless;

import java.util.List;

public class KeyValue2 {

    private String domain;
    private List<String> email;
    private String urls;
    private List<String> scriptUrls;

    private String metadata;

    public List<String> getHrefLinks() {
        return hrefLinks;
    }

    public String getMetadata() {
        return metadata;
    }

    public void setMetadata(String metadata) {
        this.metadata = metadata;
    }

    public void setHrefLinks(List<String> hrefLinks) {
        this.hrefLinks = hrefLinks;
    }

    private List<String> hrefLinks;

    public List<String> getScriptUrls() {
        return scriptUrls;
    }

    public void setScriptUrls(List<String> scriptUrls) {
        this.scriptUrls = scriptUrls;
    }

    public String getDomain() {
        return domain;
    }

    public void setDomain(String domain) {
        this.domain = domain;
    }

    public List<String> getEmail() {
        return email;
    }

    public void setEmail(List<String> email) {
        this.email = email;
    }

    public String getUrls() {
        return urls;
    }

    public void setUrls(String urls) {
        this.urls = urls;
    }

}


