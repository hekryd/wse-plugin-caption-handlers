/*
 * This code and all components (c) Copyright 2006 - 2025, Wowza Media Systems, LLC.  All rights reserved.
 * This code is licensed pursuant to the Wowza Public License version 1.0, available at www.wowza.com/legal.
 */

package com.wowza.wms.plugin.captions.transcoder;

import com.wowza.wms.logging.WMSLoggerFactory;
import com.wowza.wms.plugin.captions.audio.SpeechHandler;
import com.wowza.wms.plugin.captions.azure.AzureSpeechToTextHandler;
import com.wowza.wms.plugin.captions.caption.CaptionHandler;
import com.wowza.wms.plugin.captions.caption.DelayedStreamCaptionHandler;
import com.wowza.wms.plugin.captions.stream.DelayedStream;
import com.wowza.wms.application.IApplicationInstance;
import com.wowza.wms.transcoder.model.LiveStreamTranscoder;
import com.wowza.wms.transcoder.model.TranscoderSessionAudio;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Executors;

import static com.wowza.wms.plugin.captions.ModuleCaptionsBase.DELAYED_STREAM_SUFFIX;

public abstract class AudioResamplingTranscoderActionListener extends CaptionsTranscoderActionListener
{
    protected final IApplicationInstance appInstance;
    private final Map<String, SpeechHandler> handlers;
    private final Map<String, DelayedStream> delayedStreams;
    private final com.wowza.wms.plugin.captions.mongo.Mongo mongo;
    private final String eventCollection;
    private final String eventKey;
    private final Long eventStartAtMillis;

    private static final Path resampleTemplate;

    static {
        try (InputStream in = Objects.requireNonNull(AudioResamplingTranscoderActionListener.class.getResourceAsStream("/transcoder/templates/audioResample.xml")))
        {
            resampleTemplate = new File(System.getProperty("java.io.tmpdir"), "audioResample.xml").toPath();
            resampleTemplate.toFile().deleteOnExit();
            Files.copy(in, resampleTemplate, StandardCopyOption.REPLACE_EXISTING);
        }
        catch (IOException e)
        {
            throw new RuntimeException("Failed to create temporary file for audio resampling template", e);
        }
    }

    public AudioResamplingTranscoderActionListener(IApplicationInstance appInstance, Map<String, SpeechHandler> handlers, Map<String, DelayedStream> delayedStreams, com.wowza.wms.plugin.captions.mongo.Mongo mongo, String eventCollection, String eventKey, Long eventStartAtMillis)
    {
        this.appInstance = appInstance;
        this.handlers = handlers;
        this.delayedStreams = delayedStreams;
        this.mongo = mongo;
        this.eventCollection = eventCollection;
        this.eventKey = eventKey;
        this.eventStartAtMillis = eventStartAtMillis;
    }

    @Override
    public void onInitBeforeLoadTemplate(LiveStreamTranscoder transcoder)
    {
        super.onInitBeforeLoadTemplate(transcoder);
        if (!transcoder.getStreamName().endsWith(DELAYED_STREAM_SUFFIX))
            transcoder.setTemplateName(resampleTemplate.toUri().toString());
    }

    @Override
    public void onInitStop(LiveStreamTranscoder transcoder)
    {
        String streamName = transcoder.getStreamName();
        if (streamName.endsWith(DELAYED_STREAM_SUFFIX))
            return;
        String mappedName  = streamName.replace(".stream", "");
        EventConfig eventConfig = resolveEventConfig(mappedName);
        if (eventConfig == null)
            return;

        // Check if this is the main stream based on language detection
        boolean isMainStream = isMainStream(mappedName, eventConfig);

        TranscoderSessionAudio sessionAudio = transcoder.getTranscodingSession().getSessionAudio();
        SpeechHandler speechHandler = handlers.computeIfAbsent(mappedName, k -> {
            try
            {
                DelayedStream delayedStream = delayedStreams.computeIfAbsent(mappedName,
                        name -> new DelayedStream(appInstance, streamName, Executors.newSingleThreadScheduledExecutor()));
                CaptionHandler captionHandler = DelayedStreamCaptionHandler.create(appInstance, delayedStream, mappedName, mongo,
                        eventConfig.eventCollection, eventConfig.eventKey, eventConfig.eventStartAtMillis);

            // Only create speech handler for main stream
            if (!isMainStream) {
                return null; // Non-main streams don't process audio
            }

                SpeechHandler handler = getSpeechHandler(captionHandler ,streamName);
                new Thread(handler, handler.getClass().getSimpleName() + "[" + appInstance.getContextStr() + "/" + streamName + "]")
                        .start();
                return handler;
            }
            catch (IOException e)
            {
                WMSLoggerFactory.getLoggerObj(AudioResamplingTranscoderActionListener.class, appInstance)
                        .error("AudioResamplingTranscoderActionListener.onInitStop: Failed to create SpeechHandler for stream " + streamName, e);
                return null;
            }
        });

        // Only add frame listener if we have a speech handler (main stream only)
        if (speechHandler != null) {
            TranscoderAudioFrameListener frameListener = new TranscoderAudioFrameListener(speechHandler);
            sessionAudio.addFrameListener(frameListener);
        }
    }

    private EventConfig resolveEventConfig(String streamName)
    {
        if (mongo == null)
            return new EventConfig(eventCollection, eventKey, eventStartAtMillis, null);
        if (mongo.getDatabase() == null)
            return null;

        String streamInstance = getStreamInstance(streamName);
        if (streamInstance == null)
            return null;

        try {
            LocalDate today = LocalDate.now(ZoneId.systemDefault());
            Date startOfToday = Date.from(today.atStartOfDay(ZoneId.systemDefault()).toInstant());
            Date startOfTomorrow = Date.from(today.plusDays(1).atStartOfDay(ZoneId.systemDefault()).toInstant());
            org.bson.Document eventFilter = new org.bson.Document("startAt", new org.bson.Document("$gte", startOfToday).append("$lt", startOfTomorrow))
                    .append("stream.instance", streamInstance)
                    .append("captions.enabled", true);
            org.bson.Document eventDoc = mongo.getDatabase().getCollection("events").find(eventFilter).first();
            if (eventDoc == null) {
                WMSLoggerFactory.getLoggerObj(AudioResamplingTranscoderActionListener.class, appInstance)
                        .info("AudioResamplingTranscoderActionListener: no caption event found for stream " + streamName
                                + " with startAt on " + today + " and stream.instance " + streamInstance);
                return null;
            }

            List<String> enabledLanguages = eventDoc.getList("enabledLanguages", String.class);
            onEventSelected(eventDoc);
            return new EventConfig("events", eventDoc.getString("eventKey"), getStartAtMillis(eventDoc.get("startAt")), enabledLanguages);
        }
        catch (Exception e) {
            WMSLoggerFactory.getLoggerObj(AudioResamplingTranscoderActionListener.class, appInstance)
                    .error("AudioResamplingTranscoderActionListener: event lookup failed for stream " + streamName, e);
            return null;
        }
    }

    private String getStreamInstance(String streamName)
    {
        if (streamName == null)
            return null;
        String normalizedName = streamName.replace(".stream", "");
        int separator = normalizedName.indexOf('_');
        return separator > 0 ? normalizedName.substring(0, separator) : null;
    }

    private Long getStartAtMillis(Object startAtValue)
    {
        if (startAtValue instanceof Date)
            return ((Date) startAtValue).getTime();
        if (startAtValue instanceof Number)
            return ((Number) startAtValue).longValue();
        if (startAtValue instanceof String) {
            try {
                return java.time.Instant.parse((String) startAtValue).toEpochMilli();
            }
            catch (Exception ignore) {}
        }
        return null;
    }

    private boolean isMainStream(String streamName, EventConfig eventConfig) {
        //wow-03 cant be mainstream only wow-01
        if (mongo == null)
            return true; // Default to true if no mongo instance provided

        // If configuration explicitly marks this instance as non-main, don't process audio
        try {
            if (!mongo.isMainServer())
                return false;
        } catch (Exception ignore) {}

        try {
            String streamLanguage = null;
            if (streamName != null) {
                String[] parts = streamName.split("_");
                if (parts.length > 1)
                    streamLanguage = parts[1];
            }

            if (streamLanguage == null || eventConfig.enabledLanguages == null || eventConfig.enabledLanguages.isEmpty())
                return true;
            return streamLanguage.equals(eventConfig.enabledLanguages.get(0));
        } catch (Exception e) {
            return true;
        }

    }

    protected void onEventSelected(org.bson.Document eventDoc) {}

    private static class EventConfig
    {
        private final String eventCollection;
        private final String eventKey;
        private final Long eventStartAtMillis;
        private final List<String> enabledLanguages;

        private EventConfig(String eventCollection, String eventKey, Long eventStartAtMillis, List<String> enabledLanguages)
        {
            this.eventCollection = eventCollection;
            this.eventKey = eventKey;
            this.eventStartAtMillis = eventStartAtMillis;
            this.enabledLanguages = enabledLanguages;
        }
    }

    public abstract SpeechHandler getSpeechHandler(CaptionHandler captionHandler,String streamName) throws IOException;

    @Override
    public void onShutdownStart(LiveStreamTranscoder transcoder)
    {
        String mappedName  = transcoder.getStreamName().replace(".stream", "");
        handlers.computeIfPresent(mappedName, (k, handler) -> {
            handler.close();
            return null;
        });
    }
}
