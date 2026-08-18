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

        // Check if this is the main stream based on language detection
        boolean isMainStream = isMainStream(mappedName);

        TranscoderSessionAudio sessionAudio = transcoder.getTranscodingSession().getSessionAudio();
        SpeechHandler speechHandler = handlers.computeIfAbsent(mappedName, k -> {
            try
            {
                DelayedStream delayedStream = delayedStreams.computeIfAbsent(mappedName,
                        name -> new DelayedStream(appInstance, streamName, Executors.newSingleThreadScheduledExecutor()));
                CaptionHandler captionHandler = DelayedStreamCaptionHandler.create(appInstance, delayedStream, mappedName, mongo, eventCollection, eventKey, eventStartAtMillis);

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

    private boolean isMainStream(String streamName) {
        //wow-03 cant be mainstream only wow-01
        if (mongo == null)
            return true; // Default to true if no mongo instance provided

        // If configuration explicitly marks this instance as non-main, don't process audio
        try {
            if (!mongo.isMainServer())
                return false;
        } catch (Exception ignore) {}

        if (mongo.getDatabase() == null)
            return true; // Default to true if no mongo database connection

        try {
            String streamLanguage = null;
            if (streamName != null) {
                String[] parts = streamName.split("_");
                if (parts.length > 1)
                    streamLanguage = parts[1];
            }

            if (streamLanguage == null)
                return true; // Default to true if can't parse language

            String eventColl = eventCollection;
            if (eventColl == null) {
                // Prefer the single-collection layout
                try {
                    var names = mongo.getDatabase().listCollectionNames().into(new java.util.ArrayList<>());
                    if (names.contains("events")) eventColl = "events";
                } catch (Exception ignore) {}
            }

            org.bson.Document foundEvent = null;
            if (eventColl != null) {
                try {
                    var coll = mongo.getDatabase().getCollection(eventColl);
                    if (eventKey != null) {
                        foundEvent = coll.find(new org.bson.Document("eventKey", eventKey)).first();
                    }

                    if (foundEvent == null) {
                    for (org.bson.Document doc : coll.find()) {
                        try {
                            org.bson.Document streamDoc = doc.get("stream", org.bson.Document.class);
                            String instancePart = null;
                            try {
                                String[] parts = streamName.split("_");
                                if (parts.length > 0) instancePart = parts[0];
                            } catch (Exception ignore) {}
                            if (streamDoc != null && instancePart != null && streamDoc.containsKey("instance") && instancePart.equals(streamDoc.getString("instance"))) {
                                foundEvent = doc;
                                break;
                            }

                            if (streamLanguage != null && doc.containsKey("languages")) {
                                org.bson.Document languages = doc.get("languages", org.bson.Document.class);
                                if (languages != null && languages.containsKey(streamLanguage)) {
                                    foundEvent = doc;
                                    break;
                                }
                            }
                        } catch (Exception ignore) {}
                    }
                    }
                } catch (Exception ignore) {}
            }

            if (foundEvent != null) {
                try {
                    org.bson.Document captions = foundEvent.get("captions", org.bson.Document.class);
                    java.util.List<String> enabledLanguages = captions == null
                            ? null : captions.getList("enabledLanguages", String.class);
                    if (enabledLanguages != null && !enabledLanguages.isEmpty()) {
                        String detectedLanguageKey = enabledLanguages.get(0);
                        return streamLanguage.equals(detectedLanguageKey);
                    }
                } catch (Exception ignore) {}
            }
        } catch (Exception e) {
            // Log error but default to true to not break existing functionality
        }

        return true; // Default to true if detection fails
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
