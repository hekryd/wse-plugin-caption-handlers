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
import java.util.UUID;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.Future;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.LinkedHashMap;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicBoolean;
import com.mongodb.client.result.InsertOneResult;
import org.bson.types.ObjectId;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.changestream.FullDocument;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.model.changestream.OperationType;
import org.bson.Document;
import com.mongodb.client.MongoCursor;

import static com.wowza.wms.plugin.captions.ModuleCaptionsBase.PROP_CAPTIONS_DEBUG_LOG;
import static com.wowza.wms.plugin.captions.caption.CaptionHelper.dotNetEpoch;

public class DelayedStreamCaptionHandler implements CaptionHandler
{
    private static final Class<DelayedStreamCaptionHandler> CLASS = DelayedStreamCaptionHandler.class;
    private static final String CLASS_NAME = CLASS.getSimpleName();
    private static final int DEFAULT_WORDS_PER_MINUTE = 150;

    private static final int MAX_STREAM_CACHE_SIZE = 100;
    private static final int MAX_PUBLISHED_CACHE_SIZE = 10000;

    private final DelayedStream delayedStream;
    private final WMSLogger logger;
    private final boolean debugLog;
    private final Mongo mongo;
    private final String streamName;
    private final String eventCollectionName;
    private boolean isMainStream = false;

    private int wordsPerMinute = DEFAULT_WORDS_PER_MINUTE;
    private final ExecutorService watcherExecutor;

    // FIX #1: Use AtomicBoolean for compareAndSet to eliminate race condition
    private final AtomicBoolean watcherRunning = new AtomicBoolean(false);

    // FIX #3: Use ConcurrentHashMap + computeIfAbsent to eliminate non-atomic containsKey/get
    private final ConcurrentMap<String, String> streamToEventCollection = new ConcurrentHashMap<>();

    private static class PendingCaption {
        final ObjectId id;
        final long insertedAt;
        PendingCaption(ObjectId id, long insertedAt) { this.id = id; this.insertedAt = insertedAt; }
    }

    // FIX #2: Wrap key in a value object to handle timecode collisions explicitly
    private static class PendingCaptionSlot {
        // Use a small list to handle multiple captions at the same absolute timecode
        final java.util.concurrent.ConcurrentLinkedQueue<PendingCaption> captions = new java.util.concurrent.ConcurrentLinkedQueue<>();
    }
    private final ConcurrentMap<Long, PendingCaptionSlot> pendingCaptionByAbsTime = new ConcurrentHashMap<>();

    private final ScheduledExecutorService cleanupExecutor = Executors.newSingleThreadScheduledExecutor(r -> new Thread(r, "CaptionPendingCleanup"));
    private ScheduledFuture<?> cleanupFuture;
    private volatile MongoCursor<ChangeStreamDocument<Document>> watcherCursor;
    private volatile Future<?> watcherFuture;

    // FIX #5 + #7: final (not volatile), bounded LinkedHashMap with correct eviction
    private final Map<ObjectId, Long> publishedCaptionIds = Collections.synchronizedMap(
        new LinkedHashMap<ObjectId, Long>(16, 0.75f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<ObjectId, Long> eldest) {
                // Size-based eviction only — time-based eviction is handled by the cleanup task
                // because removeEldestEntry only fires on put() and only removes one entry at a time,
                // making it unreliable for time-based expiry of non-eldest entries.
                return size() > MAX_PUBLISHED_CACHE_SIZE;
            }
        }
    );

    // FIX #4: Private constructor — use static factory to separate init from thread/DB work
    private DelayedStreamCaptionHandler(IApplicationInstance appInstance, DelayedStream delayedStream, String streamName, Mongo mongo, String eventCollectionName)
    {
        this.delayedStream = delayedStream;
        logger = WMSLoggerFactory.getLoggerObj(DelayedStreamCaptionHandler.class, appInstance);
        debugLog = appInstance.getProperties().getPropertyBoolean(PROP_CAPTIONS_DEBUG_LOG, false);
        this.streamName = streamName;
        this.mongo = mongo;
        this.eventCollectionName = eventCollectionName;
        this.watcherExecutor = Executors.newSingleThreadExecutor(r -> new Thread(r, "CaptionChangeWatcher-" + streamName));
    }

    /**
     * FIX #4: Static factory method — separates field init from thread/DB operations.
     * Throws IllegalStateException on failure so callers cannot accidentally use a null
     * reference. Resources are always cleaned up before the exception propagates.
     */
    public static DelayedStreamCaptionHandler create(IApplicationInstance appInstance, DelayedStream delayedStream, String streamName, Mongo mongo, String eventCollectionName)
    {
        DelayedStreamCaptionHandler handler = new DelayedStreamCaptionHandler(appInstance, delayedStream, streamName, mongo, eventCollectionName);
        try {
            handler.init();
        } catch (Exception e) {
            handler.logger.error(CLASS_NAME + ".create: init failed, releasing resources: " + e.getMessage(), e);
            try { handler.close(); } catch (Exception ignore) {}
            throw new IllegalStateException(CLASS_NAME + ".create: failed to initialise handler for stream '" + streamName + "'", e);
        }
        return handler;
    }

    private void init()
    {
        detectMainStream();

        if (this.mongo != null && this.mongo.getDatabase() != null) {
            startChangeStreamWatcher();
            startPendingCleanupTask();
        }

        registerPublishListener();
    }

    private void detectMainStream()
    {
        try {
            String streamLanguage = null;
            try {
                if (streamName != null) {
                    String[] parts = streamName.split("_");
                    if (parts.length > 1)
                        streamLanguage = parts[1];
                }
            } catch (Exception ignored) {}

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
                            logger.error(CLASS_NAME + ".detectMainStream: languageConfig parsing error", e);
                        }
                        this.isMainStream = (detectedLanguageKey != null && streamLanguage != null && streamLanguage.equals(detectedLanguageKey));
                        // FIX #8: was logger.error for a non-error informational message
                        logger.info(CLASS_NAME + ".detectMainStream: firstLanguageKey=" + detectedLanguageKey + " mainStream=" + this.isMainStream);
                    }
                } catch (Exception e) {
                    logger.error(CLASS_NAME + ".detectMainStream: language detection failed: " + e.getMessage(), e);
                }
            }
        } catch (Exception e) {
            logger.error(CLASS_NAME + ".detectMainStream: error detecting main stream: " + e.getMessage(), e);
        }
    }

    private void registerPublishListener()
    {
        try {
            this.delayedStream.setPublishListener((absTimecode, packet) -> {
                try {
                    // FIX: Use merge() to atomically drain one caption from the slot.
                    // remove() + put() was a race: handleCaption could computeIfAbsent a new slot
                    // between those two calls and the put() would silently overwrite it.
                    final ObjectId[] captionIdHolder = { null };
                    pendingCaptionByAbsTime.compute(absTimecode, (k, slot) -> {
                        if (slot == null) return null;
                        PendingCaption pc = slot.captions.poll();
                        if (pc != null) captionIdHolder[0] = pc.id;
                        // return null to remove the entry if the queue is now empty,
                        // or the slot (with remaining items) if it still has entries
                        return slot.captions.isEmpty() ? null : slot;
                    });
                    ObjectId id = captionIdHolder[0];

                    String captionsDbName = resolveCaptionsDbName();
                    String eventCollection = eventCollectionName != null ? eventCollectionName : resolveEventCollectionForStream();

                    if (id != null) {
                        mongo.getClient().getDatabase(captionsDbName).getCollection(eventCollection)
                                .updateOne(new Document("_id", id), new Document("$set", new Document("published", true).append("publishedAt", new Date())));
                        if (debugLog)
                            logger.info(CLASS_NAME + ".publishListener: marked published for id=" + id + " db=" + captionsDbName + " coll=" + eventCollection);
                    } else {
                        // Fallback: find by publishTime within a small range
                        try {
                            Date publishDate = Date.from(CaptionHelper.epochInstantFromMillis(absTimecode));
                            long from = publishDate.getTime() - 1000L;
                            long to = publishDate.getTime() + 1000L;
                            Document timeRange = new Document("$gte", new Date(from)).append("$lte", new Date(to));
                            Document filter = new Document("publishTime", timeRange).append("published", new Document("$ne", true));
                            var updateRes = mongo.getClient().getDatabase(captionsDbName).getCollection(eventCollection)
                                    .updateOne(filter, new Document("$set", new Document("published", true).append("publishedAt", new Date())));
                            if (debugLog)
                                logger.info(CLASS_NAME + ".publishListener: fallback update by time range for absTimecode=" + absTimecode
                                        + " db=" + captionsDbName + " coll=" + eventCollection
                                        + " modifiedCount=" + (updateRes != null ? updateRes.getModifiedCount() : 0));
                        } catch (Exception ex) {
                            logger.error(CLASS_NAME + ".publishListener: fallback update failed: " + ex.getMessage(), ex);
                        }
                    }
                } catch (Exception e) {
                    logger.error(CLASS_NAME + ".publishListener: error marking shown: " + e.getMessage(), e);
                }
            });
        } catch (Exception e) {
            logger.error(CLASS_NAME + ".registerPublishListener: failed to register publish listener: " + e.getMessage(), e);
        }
    }

    @Override
    public void handleCaption(Caption caption)
    {
        if (debugLog)
            logger.info(CLASS_NAME + ".handleCaption: caption = " + caption);
        if (delayedStream == null || !this.isMainStream)
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

        if (mongo != null && mongo.getClient() != null && isMainStream) {
            try {
                logger.info(CLASS_NAME + ".handleCaption: main stream found, persisting caption to MongoDB");
                String captionsDbName = resolveCaptionsDbName();
                String eventCollection = eventCollectionName != null ? eventCollectionName : resolveEventCollectionForStream();

                long systemTime = System.currentTimeMillis();
                // Deterministic pairing id so captions from different languages that
                // belong to the same chunk share the same `pairId`. We base this on
                // the publish timestamp rounded to a small window and the caption start.
                long publishMillis = startOffset + captionOffset;
                long rounded = (publishMillis / 500L) * 500L; // 500ms bucket
                String pairId = UUID.nameUUIDFromBytes((String.valueOf(rounded) + "_" + caption.getBegin()).getBytes(StandardCharsets.UTF_8)).toString();

                Document doc = new Document()
                    .append("pairId", pairId)
                    .append("mainStream", streamName)
                    .append("language", caption.getLanguage())
                    .append("text", caption.getText())
                    .append("trackId", caption.getTrackId())
                    .append("videoTimecode", captionOffset)
                    .append("systemTime", systemTime)
                    .append("startTime", Date.from(CaptionHelper.epochInstantFromMillis(caption.getBegin())))
                    .append("endTime", Date.from(CaptionHelper.epochInstantFromMillis(caption.getEnd())))
                    .append("publishTime", Date.from(CaptionHelper.epochInstantFromMillis(publishMillis)))
                    .append("createdAt", new Date());

                InsertOneResult res = mongo.getClient().getDatabase(captionsDbName).getCollection(eventCollection).insertOne(doc);
                if (res != null && res.getInsertedId() != null) {
                    ObjectId id = res.getInsertedId().asObjectId().getValue();
                    long absTime = startOffset + captionOffset;

                    // FIX #2: Use computeIfAbsent + queue to safely accumulate multiple captions at the same timecode
                    PendingCaptionSlot slot = pendingCaptionByAbsTime.computeIfAbsent(absTime, k -> new PendingCaptionSlot());
                    slot.captions.add(new PendingCaption(id, System.currentTimeMillis()));

                    if (debugLog)
                        logger.info(CLASS_NAME + ".handleCaption: persisted caption id=" + id + " absTime=" + absTime + " DB=" + captionsDbName + " coll=" + eventCollection);
                } else {
                    logger.info(CLASS_NAME + ".handleCaption: persisted caption to MongoDB DB=" + captionsDbName + " collection=" + eventCollection);
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
        // FIX #1: compareAndSet ensures only one watcher thread ever starts, atomically
        if (!watcherRunning.compareAndSet(false, true)) return;

        watcherFuture = watcherExecutor.submit(() -> {
            try {
                String captionsDbName = resolveCaptionsDbName();
                String eventCollection = eventCollectionName != null ? eventCollectionName : resolveEventCollectionForStream();
                MongoCollection<Document> coll = mongo.getClient().getDatabase(captionsDbName).getCollection(eventCollection);
                var changeStream = coll.watch().fullDocument(FullDocument.UPDATE_LOOKUP);
                MongoCursor<ChangeStreamDocument<Document>> cursor = null;
                try {
                    cursor = changeStream.iterator();
                    watcherCursor = cursor;
                    while (watcherRunning.get()) {
                        ChangeStreamDocument<Document> change = null;
                        try {
                            change = cursor.tryNext();
                        } catch (NoSuchMethodError | UnsupportedOperationException ignored) {
                            if (!watcherRunning.get()) break;
                            if (!cursor.hasNext()) {
                                Thread.sleep(200);
                                continue;
                            }
                            change = cursor.next();
                        }
                        if (change == null) {
                            Thread.sleep(200);
                            continue;
                        }

                        Document full = change.getFullDocument();
                        if (full == null)
                            continue;

                        ObjectId captionId = full.getObjectId("_id");
                        boolean isPublished = full.getBoolean("published", false);
                        Date publishedAt = full.getDate("publishedAt");

                        // Handle UPDATE and REPLACE events
                        try {
                            OperationType opType = change.getOperationType();
                            if (opType == OperationType.UPDATE || opType == OperationType.REPLACE) {
                                String language = full.getString("language");
                                String text = full.getString("text");
                                int trackId = full.containsKey("trackId") ? full.getInteger("trackId", 99) : 99;

                                AMFDataObj amfDataU = new AMFDataObj();
                                amfDataU.put("text", new AMFDataItem(text));
                                amfDataU.put("language", new AMFDataItem(language));
                                amfDataU.put("trackid", new AMFDataItem(trackId));

                                AMFDataList dataListU = new AMFDataList();
                                dataListU.add(new AMFDataItem("onTextData"));
                                dataListU.add(amfDataU);
                                byte[] dataU = dataListU.serialize();

                                long nowTimecodeU = delayedStream.getPublishedStreamTimecode();
                                AMFPacket packetU = new AMFPacket(IVHost.CONTENTTYPE_DATA, 0, dataU);
                                packetU.setAbsTimecode(nowTimecodeU + 1);
                                delayedStream.writePacket(packetU);

                                if (debugLog)
                                    logger.info(CLASS_NAME + ".changeWatcher: applied text update for id=" + captionId + " stream=" + streamName);
                                continue;
                            }
                        } catch (Exception e) {
                            logger.error(CLASS_NAME + ".changeWatcher: failed to handle update event: " + e.getMessage(), e);
                        }

                        // FIX #9 (was #6 / localPublishedCache capture): access the final field directly — no captured reference
                        if (isPublished && publishedAt != null && captionId != null && !publishedCaptionIds.containsKey(captionId)) {
                            publishedCaptionIds.put(captionId, System.currentTimeMillis());

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

                            long nowTimecode = delayedStream.getPublishedStreamTimecode();
                            AMFPacket packet = new AMFPacket(IVHost.CONTENTTYPE_DATA, 0, data);
                            packet.setAbsTimecode(nowTimecode + 1);
                            delayedStream.writePacket(packet);

                            if (debugLog)
                                logger.info(CLASS_NAME + ".changeWatcher: instantly published caption for stream " + streamName + " id=" + captionId);
                        }
                    }
                } finally {
                    try { if (cursor != null) cursor.close(); } catch (Exception ignore) {}
                    watcherCursor = null;
                }
            } catch (Exception e) {
                logger.error(CLASS_NAME + ".startChangeStreamWatcher: watcher failed: " + e.getMessage(), e);
            }
        });
    }

    public void stopChangeStreamWatcher() {
        watcherRunning.set(false);
        try {
            if (watcherCursor != null) {
                try { watcherCursor.close(); } catch (Exception ignore) {}
            }
            if (watcherFuture != null) {
                watcherFuture.cancel(true);
            }
            watcherExecutor.shutdownNow();
            watcherExecutor.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (Exception ignore) {}
    }

    private void startPendingCleanupTask() {
        try {
            long retentionMillis = TimeUnit.MINUTES.toMillis(10);
            cleanupFuture = cleanupExecutor.scheduleAtFixedRate(() -> {
                try {
                    long now = System.currentTimeMillis();

                    // Cleanup pending captions
                    var iterator = pendingCaptionByAbsTime.entrySet().iterator();
                    while (iterator.hasNext()) {
                        var entry = iterator.next();
                        PendingCaptionSlot slot = entry.getValue();
                        if (slot != null) {
                            // Remove individual expired entries from the slot queue
                            slot.captions.removeIf(pc -> pc != null && (now - pc.insertedAt) > retentionMillis);
                            // Remove the slot entirely if it's empty
                            if (slot.captions.isEmpty()) {
                                iterator.remove();
                            }
                        } else {
                            iterator.remove();
                        }
                    }

                    // FIX #5: Time-based eviction of publishedCaptionIds lives here exclusively,
                    // since removeEldestEntry can only evict one entry per put and only the eldest.
                    synchronized (publishedCaptionIds) {
                        var pubIterator = publishedCaptionIds.entrySet().iterator();
                        while (pubIterator.hasNext()) {
                            var entry = pubIterator.next();
                            Long timestamp = entry.getValue();
                            if (timestamp != null && (now - timestamp) > TimeUnit.MINUTES.toMillis(5)) {
                                pubIterator.remove();
                            }
                        }
                    }
                } catch (Exception e) {
                    if (debugLog)
                        logger.warn(CLASS_NAME + ".cleanupTask: error during cleanup: " + e.getMessage());
                }
            }, 1, 1, TimeUnit.MINUTES);
        } catch (Exception e) {
            if (debugLog)
                logger.warn(CLASS_NAME + ".startPendingCleanupTask: " + e.getMessage());
        }
    }

    public void close() {
        try { stopChangeStreamWatcher(); } catch (Exception ignore) {}
        try { delayedStream.setPublishListener(null); } catch (Exception ignore) {}
        try {
            if (cleanupFuture != null) cleanupFuture.cancel(true);
            cleanupExecutor.shutdownNow();
            cleanupExecutor.awaitTermination(3, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (Exception ignore) {}
        try { pendingCaptionByAbsTime.clear(); } catch (Exception ignore) {}
        try { streamToEventCollection.clear(); } catch (Exception ignore) {}
        try { publishedCaptionIds.clear(); } catch (Exception ignore) {}
    }

    /**
     * FIX: resolveEventCollectionForStream no longer uses computeIfAbsent for the cache lookup.
     * computeIfAbsent holds a map-bucket lock for the entire duration of the lambda, which would
     * stall any other thread hashing to the same bucket while MongoDB I/O is in flight.
     * Instead we do the lookup unconditionally then use putIfAbsent — only the winning value
     * is stored, and the map is never locked during network calls.
     *
     * FIX #6b: The JSON substring fallback is replaced with strict field-only checks to
     * prevent false positives where the stream name appears as a substring of unrelated values.
     */
    private String resolveEventCollectionForStream() {
        String cached = streamToEventCollection.get(streamName);
        if (cached != null) return cached;

        String resolved = lookupCollectionInMongo(streamName);

        // putIfAbsent: if another thread beat us here, use their result and discard ours
        String existing = streamToEventCollection.putIfAbsent(streamName, resolved);
        return existing != null ? existing : resolved;
    }

    private String lookupCollectionInMongo(String key) {
        if (mongo == null || mongo.getDatabase() == null)
            return key;

        try {
            var eventsDb = mongo.getDatabase();
            for (String collName : eventsDb.listCollectionNames()) {
                try {
                    Document eventConfig = eventsDb.getCollection(collName).find(new Document("_id", "event_config")).first();
                    if (eventConfig == null)
                        continue;
                    if (key.equals(eventConfig.getString("streamName")))
                        return collName;
                    if (key.equals(eventConfig.getString("stream")))
                        return collName;
                    if (eventConfig.containsKey("streams")) {
                        try {
                            var list = eventConfig.get("streams", java.util.List.class);
                            if (list != null && list.contains(key))
                                return collName;
                        } catch (Exception ignore) {}
                    }
                } catch (Exception e) {
                    if (debugLog)
                        logger.warn(CLASS_NAME + ".lookupCollectionInMongo: error reading collection " + collName + ": " + e.getMessage());
                }
            }
        } catch (Exception e) {
            if (debugLog)
                logger.warn(CLASS_NAME + ".lookupCollectionInMongo: " + e.getMessage());
        }

        // Fallback: treat the stream name itself as the collection name
        return key;
    }

    /**
     * Derives the Captions DB name from the Events DB name.
     * Throws IllegalStateException if the DB name cannot be resolved so callers
     * get a clear error rather than a NullPointerException from getDatabase(null).
     */
    private String resolveCaptionsDbName() {
        String eventsDbName = mongo.getDatabase() != null ? mongo.getDatabase().getName() : null;
        if (eventsDbName == null)
            throw new IllegalStateException(CLASS_NAME + ".resolveCaptionsDbName: mongo database is not available");
        String customer = null;
        if (eventsDbName.startsWith("Events_")) {
            customer = eventsDbName.substring("Events_".length());
        } else if (eventsDbName.startsWith("Customer_")) {
            customer = eventsDbName.substring("Customer_".length());
        }
        return customer != null ? "Captions_" + customer : eventsDbName;
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