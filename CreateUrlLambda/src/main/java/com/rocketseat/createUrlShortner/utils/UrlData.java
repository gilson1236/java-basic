package com.rocketseat.createUrlShortner.utils;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;


public class UrlData {
    private String originalUrl;
    private long expirationTime;

    public UrlData(){

    }

    public UrlData(String originalUrl, long expirationTime){
        this.originalUrl = originalUrl;
        this.expirationTime = expirationTime;
    }

    public String getOriginalUrl() {
        return originalUrl;
    }

    public void setOriginalUrl(String originalUrl) {
        this.originalUrl = originalUrl;
    }

    public long getExpirationTime() {
        return expirationTime;
    }

    public void setExpirationTime(long expirationTime) {
        this.expirationTime = expirationTime;
    }
}
