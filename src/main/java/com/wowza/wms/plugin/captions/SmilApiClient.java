package com.wowza.wms.plugin.captions;

import com.wowza.wms.application.IApplicationInstance;
import com.wowza.wms.logging.WMSLogger;
import com.wowza.wms.logging.WMSLoggerFactory;

import java.io.FileInputStream;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.URI;
import java.time.Duration;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Properties;

public class SmilApiClient {
    private static final WMSLogger logger = WMSLoggerFactory.getLogger(SmilApiClient.class);
    private static final HttpClient CLIENT = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(10))
            .build();

    // Path mirrors Mongo.loadConfiguration default
    private static final String DEFAULT_ENV_PATH = "/usr/local/WowzaStreamingEngine/custom-plugin-resources/env.properties";

    //array that keeps track of the smil files we have created, so we can delete them on app stop. In format customer_instance.
    private static final List<String> createdSmilFiles = new ArrayList<>();

    private static String escape(String s) {
        return s == null ? "" : s.replace("\\", "\\\\").replace("\"", "\\\"");
    }


    public static class SmilStream {
        public String systemLanguage;
        public String src;
        public String systemBitrate;
        public String type = "video";
        public String audioBitrate;
        public String videoBitrate;
        public String width;
        public String height;

        public SmilStream(String systemLanguage, String src, String systemBitrate, String audioBitrate, String videoBitrate, String width, String height) {
            this.systemLanguage = systemLanguage;
            this.src = src;
            this.systemBitrate = systemBitrate;
            this.audioBitrate = audioBitrate;
            this.videoBitrate = videoBitrate;
            this.width = width;
            this.height = height;
        }

        public String toJson() {
            return "{" +
                    "\"systemLanguage\":\"" + escape(systemLanguage) + "\"," +
                    "\"src\":\"" + escape(src) + "\"," +
                    "\"systemBitrate\":\"" + escape(systemBitrate) + "\"," +
                    "\"type\":\"" + escape(type) + "\"," +
                    "\"audioBitrate\":\"" + escape(audioBitrate) + "\"," +
                    "\"videoBitrate\":\"" + escape(videoBitrate) + "\"," +
                    "\"width\":\"" + escape(width) + "\",\"height\":\"" + escape(height) + "\"}";
        }
    }

    private static String readAuthHeader() {
        try (FileInputStream fis = new FileInputStream(DEFAULT_ENV_PATH)) {
            Properties p = new Properties();
            p.load(fis);
            String user = p.getProperty("wowza.api.username");
            String pass = p.getProperty("wowza.api.password");
            if (user != null && pass != null) {
                String cred = user + ":" + pass;
                return "Basic " + Base64.getEncoder().encodeToString(cred.getBytes(StandardCharsets.UTF_8));
            }
        } catch (Exception e) {
            // ignore: file may not exist yet
        }
        return null;
    }

    public static String postJson(String url, String json, String authHeader) throws Exception {
        HttpRequest.Builder builder = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .timeout(Duration.ofSeconds(15))
                .header("Accept", "application/json")
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(json, StandardCharsets.UTF_8));

        if (authHeader != null && !authHeader.isEmpty()) {
            builder.header("Authorization", authHeader);
        }

        HttpRequest req = builder.build();
        HttpResponse<String> resp = CLIENT.send(req, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
        return resp.body();
    }

public static String createSmilForApplication(IApplicationInstance appInstance, String smilName, List<SmilStream> streams, List<String> languages, String eventInstance, boolean showCaptionsInEvent, String smilLanguage) throws Exception {
    String appName = appInstance.getApplication().getName().replace("target", "playout");
    String url = "http://localhost:8087/v2/servers/_defaultServer_/vhosts/_defaultVHost_/applications/" + appName + "/smilfiles/" + smilName;

    StringBuilder sb = new StringBuilder();
    sb.append("{");
    // Root required fields
    sb.append("\"name\":\"").append(smilName).append("\",");
    sb.append("\"title\":\"").append(smilName).append("\",");
    sb.append("\"serverName\":\"_defaultServer_\",");
    
    sb.append("\"smilStreams\":[");
    List<String> parts = new ArrayList<>();
    
    // Add existing video/audio streams
    for (SmilStream s : streams) {
        parts.add(s.toJson());
    }

    // Add textstream entries for requested languages
    if (languages != null && !languages.isEmpty() && showCaptionsInEvent) {
        int index = streams.size();
        for (String lang : languages) {
            String textSrcWithLang = eventInstance + "_" + smilLanguage + "_delayed_1080p";
            StringBuilder ts = new StringBuilder();
            ts.append("{");
            ts.append("\"systemLanguage\":\"").append(escape(lang)).append("\",");
            ts.append("\"src\":\"").append(escape(textSrcWithLang)).append("\",");
            ts.append("\"type\":\"textstream\",");
            // Move caption fields to top-level properties
            ts.append("\"isWowzaCaptionStream\":\"true\",");

            ts.append("\"wowzaCaptionIngestType\":\"onTextData events in live streams\",");
            // Add other required API fields
            ts.append("\"dur\":\"\",");
            ts.append("\"ngrp\":\"\",");
            ts.append("\"keyFrameOnly\":\"false\",");
            ts.append("\"systemBitrate\":\"0\",");
            ts.append("\"videoCodecId\":\"\",");
            ts.append("\"version\":\"\",");
            ts.append("\"audioBitrate\":\"0\",");
            ts.append("\"audioCodecId\":\"\",");
            ts.append("\"videoBitrate\":\"0\",");
            ts.append("\"videoOnly\":\"false\",");
            ts.append("\"audioOnly\":\"false\",");
            ts.append("\"width\":\"0\",");
            ts.append("\"height\":\"0\",");
            ts.append("\"idx\":").append(index++).append(",");
            ts.append("\"begin\":\"\",");
            ts.append("\"title\":\"\"");
            ts.append("}");
            parts.add(ts.toString());
        }
    }
    
    sb.append(String.join(",", parts));
    sb.append("]}");

    try {
        deleteSmilForApplication(appInstance, smilName);
    } catch (Exception e) {
        logger.warn("Failed to delete existing SMIL before create: " + smilName, e);
    }

    String auth = readAuthHeader();
    return postJson(url, sb.toString(), auth);
}


    public static String deleteSmilForApplication(IApplicationInstance appInstance) throws Exception {
        String appName = appInstance.getApplication().getName();

        if (createdSmilFiles.isEmpty()) {
            logger.info("No SMIL files to delete for application: " + appName);
            return "No SMIL files to delete.";
        }

        List<String> deleted = new ArrayList<>();
        for (String smilName : new ArrayList<>(createdSmilFiles)) {
            try {
                String url = "http://localhost:8087/v2/servers/_defaultServer_/vhosts/_defaultVHost_/applications/" + appName + "_playout" + "/smilfiles/" + smilName;

                HttpRequest.Builder builder = HttpRequest.newBuilder()
                        .uri(URI.create(url))
                        .timeout(Duration.ofSeconds(15))
                        .header("Accept", "application/json")
                        .DELETE();

                String auth = readAuthHeader();
                if (auth != null && !auth.isEmpty()) {
                    builder.header("Authorization", auth);
                }

                HttpRequest req = builder.build();
                HttpResponse<String> resp = CLIENT.send(req, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
                logger.info("Deleted SMIL: " + smilName + " response: " + resp.body());
                deleted.add(smilName);
            } catch (Exception e) {
                logger.error("Failed to delete SMIL: " + smilName, e);
            }
        }

        createdSmilFiles.removeAll(deleted);
        return "Deleted SMIL files: " + deleted.toString();
    }

    /**
     * Delete a single SMIL file for the given application (used before creating a new one).
     */
    public static String deleteSmilForApplication(IApplicationInstance appInstance, String smilName) throws Exception {
        String appName = appInstance.getApplication().getName();
        String url = "http://localhost:8087/v2/servers/_defaultServer_/vhosts/_defaultVHost_/applications/" + appName + "_playout" + "/smilfiles/" + smilName;

        HttpRequest.Builder builder = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .timeout(Duration.ofSeconds(15))
                .header("Accept", "application/json")
                .DELETE();

        String auth = readAuthHeader();
        if (auth != null && !auth.isEmpty()) {
            builder.header("Authorization", auth);
        }

        HttpRequest req = builder.build();
        HttpResponse<String> resp = CLIENT.send(req, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
        logger.info("Deleted SMIL: " + smilName + " response: " + resp.body());
        createdSmilFiles.remove(smilName);
        return resp.body();
    }

/**
     * Convenience overload: generate standard video qualities and create SMIL.
     * Uses baseSrc as the naming base for generated stream src values.
     */
    public static String createSmilForApplication(IApplicationInstance appInstance, String smilName, String baseSrc, List<String> languages, String eventInstance, boolean showCaptionsInEvent, String smilLanguage) throws Exception {
        List<SmilStream> streams = new ArrayList<>();
        // exact qualities requested by user
        streams.add(new SmilStream(null, baseSrc + "_delayed_1080p", "4308000", "192000", "4308000", "1920", "1080"));
        streams.add(new SmilStream(null, baseSrc + "_delayed_720p", "2308000", "192000", "2308000", "1280", "720"));
        streams.add(new SmilStream(null, baseSrc + "_delayed_480p", "1054000", "128000", "1054000", "854", "480"));
        streams.add(new SmilStream(null, baseSrc + "_delayed_360p", "672000", "128000", "672000", "640", "360"));
        streams.add(new SmilStream(null, baseSrc + "_delayed_288p", "412000", "96000", "412000", "512", "288"));
        streams.add(new SmilStream(null, baseSrc + "_delayed_180p", "180000", "96000", "180000", "320", "180"));
        return createSmilForApplication(appInstance, smilName, streams, languages, eventInstance, showCaptionsInEvent,smilLanguage);
    }
}
