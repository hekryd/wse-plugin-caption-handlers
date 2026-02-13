/*
 * This code and all components (c) Copyright 2006 - 2025, Wowza Media Systems, LLC.  All rights reserved.
 * This code is licensed pursuant to the Wowza Public License version 1.0, available at www.wowza.com/legal.
 */

package com.wowza.wms.plugin.captions.caption;

import com.wowza.wms.plugin.captions.stream.DelayedStream;
import com.wowza.wms.amf.*;
import com.wowza.wms.application.IApplicationInstance;
import com.wowza.wms.logging.*;
import com.wowza.wms.vhost.IVHost;
import com.wowza.wms.plugin.captions.mongo.Mongo;
import org.bson.Document;

import java.time.*;
import java.util.Date;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import com.mongodb.client.result.InsertOneResult;
import org.bson.types.ObjectId;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.changestream.FullDocument;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import org.bson.Document;
import com.mongodb.client.MongoCursor;

import static com.wowza.wms.plugin.captions.ModuleCaptionsBase.PROP_CAPTIONS_DEBUG_LOG;
import static com.wowza.wms.plugin.captions.caption.CaptionHelper.dotNetEpoch;

public class DelayedStreamCaptionHandler implements CaptionHandler
{
    private static final Class<DelayedStreamCaptionHandler> CLASS = DelayedStreamCaptionHandler.class;
    private static final String CLASS_NAME = CLASS.getSimpleName();
    private static final int DEFAULT_WORDS_PER_MINUTE = 150;
    private final DelayedStream delayedStream;
    private final WMSLogger logger;
    private final boolean debugLog;
    private final Mongo mongo;
    private final String streamName;
    private final String eventCollectionName;
    private boolean isMainStream = false;

    private int wordsPerMinute = DEFAULT_WORDS_PER_MINUTE;
    private final ExecutorService watcherExecutor;
    private volatile boolean watcherRunning = false;
    private final Map<String, String> streamToEventCollection = new ConcurrentHashMap<>();
    private final ConcurrentMap<Long, ObjectId> pendingCaptionByAbsTime = new ConcurrentHashMap<>();

    public DelayedStreamCaptionHandler(IApplicationInstance appInstance, DelayedStream delayedStream, String streamName, Mongo mongo, String eventCollectionName)
    {
        this.delayedStream = delayedStream;
        logger = WMSLoggerFactory.getLoggerObj(DelayedStreamCaptionHandler.class, appInstance);
        debugLog = appInstance.getProperties().getPropertyBoolean(PROP_CAPTIONS_DEBUG_LOG, false);
        this.streamName = streamName;
        this.mongo = mongo;
        this.eventCollectionName = eventCollectionName;
        this.watcherExecutor = Executors.newSingleThreadExecutor(r -> new Thread(r, "CaptionChangeWatcher-" + streamName));

        // attempt to detect whether this delayed stream corresponds to the main stream
        try {
            String streamLanguage = null;
            try {
                if (streamName != null) {
                    String[] parts = streamName.split("_");
                    if (parts.length > 1)
                        streamLanguage = parts[1];
                }
            } catch (Exception ignored) {
            }

            if (this.mongo != null && this.mongo.getDatabase() != null) {
                try {
                    String eventColl = this.eventCollectionName != null ? this.eventCollectionName : resolveEventCollectionForStream();
                    Document languageConfig = this.mongo.getDatabase().getCollection(eventColl).find(new Document("_id", "language_config")).first();
                    if (languageConfig != null) {
                        String detectedLanguageKey = null;
                        try {
                            if (languageConfig.containsKey("lang")) {
                                Object langObj = languageConfig.get("lang");
                                if (langObj instanceof java.util.List) {
                                    java.util.List<?> list = (java.util.List<?>) langObj;
                                    if (!list.isEmpty()) {
                                        Object first = list.get(0);
                                        if (first instanceof org.bson.Document) {
                                            detectedLanguageKey = ((org.bson.Document) first).getString("language_key");
                                        } else if (first instanceof java.util.Map) {
                                            Object k = ((java.util.Map<?, ?>) first).get("language_key");
                                            if (k != null)
                                                detectedLanguageKey = k.toString();
                                        }
                                    }
                                }
                            }
                        } catch (Exception e) {
                            logger.error(CLASS_NAME + ".onInit: languageConfig parsing error", e);
                        }
                        this.isMainStream = (detectedLanguageKey != null && streamLanguage != null && streamLanguage.equals(detectedLanguageKey));
                            logger.error(CLASS_NAME + ".init: firstLanguageKey=" + detectedLanguageKey + " mainStream=" + this.isMainStream);
                    }
                } catch (Exception e) {
                    logger.error(CLASS_NAME + ".onInit: language detection failed: " + e.getMessage(), e);
                }
            }
        } catch (Exception e) {
            logger.error(CLASS_NAME + ".init: error detecting main stream: " + e.getMessage(), e);
        }

        // start watcher if mongo is available
        if (this.mongo != null && this.mongo.getDatabase() != null) {
            startChangeStreamWatcher();
        }
        // register as publish listener so we can mark captions shown when actually published
        try {
            this.delayedStream.setPublishListener((absTimecode, packet) -> {
                try {
                    ObjectId id = pendingCaptionByAbsTime.remove(absTimecode);
                    String eventsDbName = mongo.getDatabase() != null ? mongo.getDatabase().getName() : null;
                    String customer = null;
                    if (eventsDbName != null) {
                        if (eventsDbName.startsWith("Events_")) {
                            customer = eventsDbName.substring("Events_".length());
                        } else if (eventsDbName.startsWith("Customer_")) {
                            customer = eventsDbName.substring("Customer_".length());
                        }
                    }
                    String captionsDbName = customer != null ? "Captions_" + customer : (mongo.getDatabase() != null ? mongo.getDatabase().getName() : null);
                    String eventCollection = eventCollectionName != null ? eventCollectionName : resolveEventCollectionForStream();

                    if (id != null) {
                        if (captionsDbName != null && eventCollection != null) {
                            mongo.getClient().getDatabase(captionsDbName).getCollection(eventCollection)
                                    .updateOne(new Document("_id", id), new Document("$set", new Document("published", true).append("publishedAt", new Date())));
                            if (debugLog)
                                logger.info(CLASS_NAME + ".publishListener: marked published for id=" + id + " db=" + captionsDbName + " coll=" + eventCollection);
                        }
                    } else {
                        // Fallback: try to find the document by publishTime within a small range and mark it published
                        try {
                            if (captionsDbName != null && eventCollection != null) {
                                Date publishDate = Date.from(CaptionHelper.epochInstantFromMillis(absTimecode));
                                long from = publishDate.getTime() - 1000L;
                                long to = publishDate.getTime() + 1000L;
                                Document timeRange = new Document("$gte", new Date(from)).append("$lte", new Date(to));
                                Document filter = new Document("publishTime", timeRange).append("published", new Document("$ne", true));
                                var updateRes = mongo.getClient().getDatabase(captionsDbName).getCollection(eventCollection)
                                        .updateOne(filter, new Document("$set", new Document("published", true).append("publishedAt", new Date())));
                                if (debugLog)
                                    logger.info(CLASS_NAME + ".publishListener: fallback update by time range for absTimecode=" + absTimecode + " db=" + captionsDbName + " coll=" + eventCollection + " modifiedCount=" + (updateRes != null ? updateRes.getModifiedCount() : 0));
                            }
                        } catch (Exception ex) {
                            logger.error(CLASS_NAME + ".publishListener: fallback update failed: " + ex.getMessage(), ex);
                        }
                    }
                } catch (Exception e) {
                    logger.error(CLASS_NAME + ".publishListener: error marking shown: " + e.getMessage(), e);
                }
            });
        } catch (Exception e) {
            logger.error(CLASS_NAME + ".init: failed to register publish listener: " + e.getMessage(), e);
        }
    }

@Override
public void handleCaption(Caption caption)
{
    if (debugLog)
        logger.info(CLASS_NAME + ".handleCaption: caption = " + caption);
    if (delayedStream == null && !this.isMainStream)
        return;
    AMFDataObj amfData = new AMFDataObj();
    amfData.put("text", new AMFDataItem(caption.getText()));
    amfData.put("language", new AMFDataItem(caption.getLanguage()));
    amfData.put("trackid", new AMFDataItem(caption.getTrackId()));

    AMFDataList dataList = new AMFDataList();
    dataList.add(new AMFDataItem("onTextData"));
    dataList.add(amfData);
    byte[] data = dataList.serialize();

    long startOffset = delayedStream.getStartOffset();
    long captionOffset = caption.getBegin();
    AMFPacket packet = new AMFPacket(IVHost.CONTENTTYPE_DATA, 0, data);
    packet.setAbsTimecode(startOffset + captionOffset);
    if (debugLog)
        logger.info(CLASS_NAME + ".handleCaption: packet = " + packet + ", stream buffer: " + delayedStream.getFirstPacketTimecode() + " - " + delayedStream.getLastPacketTimecode());
    delayedStream.writePacket(packet);

    // persist caption to Mongo (if available)
    if (mongo != null && mongo.getClient() != null && isMainStream) {
        try {
            logger.info("mainStram found. Persisting caption to Mongo DB");
            String eventsDbName = mongo.getDatabase() != null ? mongo.getDatabase().getName() : null;
            String customer = null;
            if (eventsDbName != null) {
                if (eventsDbName.startsWith("Events_")) {
                    customer = eventsDbName.substring("Events_".length());
                } else if (eventsDbName.startsWith("Customer_")) {
                    customer = eventsDbName.substring("Customer_".length());
                }
            }
            String captionsDbName = customer != null ? "Captions_" + customer : mongo.getDatabase().getName();
            String eventCollection = eventCollectionName != null ? eventCollectionName : resolveEventCollectionForStream();

            // Speichere zusätzlich die absolute Systemzeit, wann das Caption erscheinen soll
            long systemTime = System.currentTimeMillis();

            Document doc = new Document()
                    .append("mainStream", streamName)
                    .append("language", caption.getLanguage())
                    .append("text", caption.getText())
                    .append("trackId", caption.getTrackId())
                    .append("videoTimecode", captionOffset)
                    .append("systemTime", systemTime)
                    .append("startTime", Date.from(CaptionHelper.epochInstantFromMillis(caption.getBegin())))
                    .append("endTime", Date.from(CaptionHelper.epochInstantFromMillis(caption.getEnd())))
                    .append("publishTime", Date.from(CaptionHelper.epochInstantFromMillis(startOffset + captionOffset)))
                    .append("createdAt", new Date());
            InsertOneResult res = mongo.getClient().getDatabase(captionsDbName).getCollection(eventCollection).insertOne(doc);
            if (res != null && res.getInsertedId() != null) {
                ObjectId id = res.getInsertedId().asObjectId().getValue();
                long absTime = startOffset + captionOffset;
                pendingCaptionByAbsTime.put(absTime, id);
                if (debugLog)
                    logger.info(CLASS_NAME + ".handleCaption: persisted caption id=" + id + " absTime=" + absTime + " DB=" + captionsDbName + " coll=" + eventCollection);
            } else {
                logger.info(CLASS_NAME + ".handleCaption: persisted caption to Mongo DB=" + captionsDbName + " collection=" + eventCollection);
            }
        } catch (Exception e) {
            logger.error(CLASS_NAME + ".handleCaption: failed to persist caption: " + e.getMessage(), e);
        }
    }
}

@Override
public int getWordsPerMinute()
{
    return wordsPerMinute;
}

@Override
public void setWordsPerMinute(int wordsPerMinute)
{
    this.wordsPerMinute = wordsPerMinute;
}

public CaptionTiming getCaptionTiming()
    {
        long startOffset = delayedStream.getStartOffset();
        long firstTC = delayedStream.getFirstPacketTimecode();
        long lastTC = delayedStream.getLastPacketTimecode();
        Instant start = dotNetEpoch.plusMillis(firstTC - startOffset);
        Instant end = dotNetEpoch.plusMillis(lastTC - startOffset);
        return new CaptionTiming(start, end);
    }

private void startChangeStreamWatcher() {
    if (watcherRunning) return;
    watcherRunning = true;
    // Set zum Verhindern doppelter Ausspielungen
    java.util.Set<ObjectId> alreadyPublished = java.util.Collections.newSetFromMap(new java.util.concurrent.ConcurrentHashMap<>());
    watcherExecutor.submit(() -> {
        try {
            String eventsDbName = mongo.getDatabase() != null ? mongo.getDatabase().getName() : null;
            String customer = null;
            if (eventsDbName != null) {
                if (eventsDbName.startsWith("Events_")) {
                    customer = eventsDbName.substring("Events_".length());
                } else if (eventsDbName.startsWith("Customer_")) {
                    customer = eventsDbName.substring("Customer_".length());
                }
            }
            String captionsDbName = customer != null ? "Captions_" + customer : (mongo.getDatabase() != null ? mongo.getDatabase().getName() : null);
            String eventCollection = eventCollectionName != null ? eventCollectionName : resolveEventCollectionForStream();
            MongoCollection<Document> coll = mongo.getClient().getDatabase(captionsDbName).getCollection(eventCollection);
            var changeStream = coll.watch().fullDocument(FullDocument.UPDATE_LOOKUP);
            try (MongoCursor<ChangeStreamDocument<Document>> cursor = changeStream.iterator()) {
                while (watcherRunning && cursor.hasNext()) {
                    ChangeStreamDocument<Document> change = cursor.next();
                    if (change == null)
                        continue;

                    // Nur im !mainStream auf published-Events reagieren
                    if (isMainStream)
                        continue;

                    Document full = change.getFullDocument();
                    if (full == null)
                        continue;

                    ObjectId captionId = full.getObjectId("_id");
                    boolean isPublished = full.getBoolean("published", false);
                    Date publishedAt = full.getDate("publishedAt");

                    // Nur published-Events verarbeiten und doppelte Ausspielung verhindern
                    if (isPublished && publishedAt != null && captionId != null && !alreadyPublished.contains(captionId)) {
                        alreadyPublished.add(captionId);

                        String language = full.getString("language");
                        String text = full.getString("text");
                        int trackId = full.containsKey("trackId") ? full.getInteger("trackId", 99) : 99;

                        AMFDataObj amfData = new AMFDataObj();
                        amfData.put("text", new AMFDataItem(text));
                        amfData.put("language", new AMFDataItem(language));
                        amfData.put("trackid", new AMFDataItem(trackId));

                        AMFDataList dataList = new AMFDataList();
                        dataList.add(new AMFDataItem("onTextData"));
                        dataList.add(amfData);
                        byte[] data = dataList.serialize();

                        // Caption mit aktuellem Stream-Zeitcode ausspielen
                        long nowTimecode = delayedStream.getPublishedStreamTimecode();
                        AMFPacket packet = new AMFPacket(IVHost.CONTENTTYPE_DATA, 0, data);
                        packet.setAbsTimecode(nowTimecode + 1); // sofort ausspielen
                        delayedStream.writePacket(packet);

                        if (debugLog)
                            logger.info(CLASS_NAME + ".changeWatcher: instantly published caption for stream " + streamName + " id=" + captionId);
                    }
                }
            }
        } catch (Exception e) {
            logger.error(CLASS_NAME + ".startChangeStreamWatcher: watcher failed: " + e.getMessage(), e);
        }
    });
}

    public void stopChangeStreamWatcher() {
        watcherRunning = false;
        watcherExecutor.shutdownNow();
    }

    private String resolveEventCollectionForStream() {
        // cached lookup
        if (streamToEventCollection.containsKey(streamName))
            return streamToEventCollection.get(streamName);

        if (mongo == null || mongo.getDatabase() == null)
            return streamName;

        try {
            var eventsDb = mongo.getDatabase();
            for (String collName : eventsDb.listCollectionNames()) {
                try {
                    Document eventConfig = eventsDb.getCollection(collName).find(new Document("_id", "event_config")).first();
                    if (eventConfig == null)
                        continue;
                    // direct fields match
                    if (eventConfig.containsKey("streamName") && streamName.equals(eventConfig.getString("streamName"))) {
                        streamToEventCollection.put(streamName, collName);
                        return collName;
                    }
                    if (eventConfig.containsKey("stream") && streamName.equals(eventConfig.getString("stream"))) {
                        streamToEventCollection.put(streamName, collName);
                        return collName;
                    }
                    // arrays containing stream name
                    if (eventConfig.containsKey("streams")) {
                        try {
                            var list = eventConfig.get("streams", java.util.List.class);
                            if (list != null && list.contains(streamName)) {
                                streamToEventCollection.put(streamName, collName);
                                return collName;
                            }
                        } catch (Exception ignore) {
                        }
                    }
                    // fallback: raw json contains streamName
                    if (eventConfig.toJson().contains(streamName)) {
                        streamToEventCollection.put(streamName, collName);
                        return collName;
                    }
                } catch (Exception e) {
                    if (debugLog)
                        logger.warn(CLASS_NAME + ".resolveEventCollectionForStream: error reading collection " + collName + ": " + e.getMessage());
                }
            }
        } catch (Exception e) {
            if (debugLog)
                logger.warn(CLASS_NAME + ".resolveEventCollectionForStream: " + e.getMessage());
        }

        // fallback to using streamName as collection
        streamToEventCollection.put(streamName, streamName);
        return streamName;
    }

    @Override
    public long getStartOffset()
    {
        return delayedStream.getStartOffset();
    }

    @Override
    public String getStreamName()
    {
        return delayedStream.getStreamName();
    }
}
