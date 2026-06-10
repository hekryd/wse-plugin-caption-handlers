/*
 * This code and all components (c) Copyright 2006 - 2025, Wowza Media Systems, LLC.  All rights reserved.
 * This code is licensed pursuant to the Wowza Public License version 1.0, available at www.wowza.com/legal.
 */

package com.wowza.wms.plugin.captions;

import com.microsoft.cognitiveservices.speech.SpeechConfig;
import com.wowza.wms.plugin.captions.audio.SpeechHandler;
import com.wowza.wms.plugin.captions.azure.AzureCaptionsTranscoderActionListener;
import com.wowza.wms.plugin.captions.stream.DelayedStream;
import com.wowza.wms.plugin.captions.stream.DelayedStreamListener;
import com.wowza.wms.plugin.captions.stream.LiveStreamPacketizerListener;
import com.wowza.wms.plugin.captions.transcoder.CaptionsTranscoderCreateListener;
import com.wowza.wms.application.IApplicationInstance;
import com.wowza.wms.logging.*;
import com.wowza.wms.stream.*;
import com.wowza.wms.timedtext.model.ITimedTextConstants;

import java.util.*;
import java.util.concurrent.*;
import org.bson.Document;
import com.wowza.wms.plugin.captions.mongo.Mongo;


public class ModuleAzureSpeechToTextCaptions extends ModuleCaptionsBase
{
    static
    {
        CLASS = ModuleAzureSpeechToTextCaptions.class;
        MODULE_NAME = CLASS.getSimpleName();
        try
        {
            Class.forName(SpeechConfig.class.getName());
        }
        catch (ClassNotFoundException e)
        {
            WMSLoggerFactory.getLogger(CLASS).error(String.format("%s exception: %s", MODULE_NAME, e), e);
        }
    }

    public static final String PROP_CAPTIONS_ENABLED = "speechToTextCaptionsEnabled";
    public static final String PROP_DEFAULT_CAPTION_LANGUAGES = ITimedTextConstants.PROP_LIVE_CAPTION_DEFAULT_LANGUAGES;
    public static final String PROP_RECOGNITION_LANGUAGE = "speechToTextRecognitionLanguage";
    public static final String PROP_PHRASE_LIST = "speechToTextPhraseList";
    public static final String PROP_PROFANITY_MASK_OPTION = "speechToTextProfanityMaskOption";
    public static final String PROP_SUBSCRIPTION_KEY = "speechToTextSubscriptionKey";
    public static final String PROP_SERVICE_REGION = "speechToTextServiceRegion";
    private final Map<String, SpeechHandler> speechHandlers = new ConcurrentHashMap<>();
    private final Map<String, DelayedStream> delayedStreams = new ConcurrentHashMap<>();
    private DelayedStreamListener delayedStreamListener;
    private String subscriptionKey;
    private String serviceRegion;
    private Mongo mongo;
    private String customer;
    private String liveEventCollection;

    //event_config
    private boolean enabled = false;
    private int added_stream_delay_in_ms = 30000;
    private List<String> enabled_languages =  Arrays.asList("de","en"); 
    private boolean showCaptionsInEvent = false;

    public void onAppCreate(IApplicationInstance appInstance)
    {
        super.onAppCreate(appInstance);
        //enabled = appInstance.getProperties().getPropertyBoolean(PROP_CAPTIONS_ENABLED, enabled);
        try
        {
            subscriptionKey = Objects.requireNonNull(appInstance.getProperties().getPropertyStr(PROP_SUBSCRIPTION_KEY), "Azure Speech Subscription Key not set");
            serviceRegion = Objects.requireNonNull(appInstance.getProperties().getPropertyStr(PROP_SERVICE_REGION), "Azure Speech Service Region not set");
        }
        catch (NullPointerException npe)
        {
            logger.error(String.format("%s.onAppCreate [%s] error: %s", MODULE_NAME, appInstance.getContextStr(), npe.getMessage()));
            enabled = false;
        }
        logger.info(String.format("%s.onAppCreate: [%s] version: %s enabled: %b", MODULE_NAME, appInstance.getContextStr(), MODULE_VERSION, enabled));
    }

    public void onAppStart(IApplicationInstance appInstance)
    {
        //logger.error(MODULE_NAME + ".onAppStart initializing MongoDB connection");
        customer = appInstance.getApplication().getName().split("_")[0];

        // New Mongo layout: database is Customer_<customer>, collection is always "events"
        mongo = new Mongo("Customer_" + customer);
        if (mongo == null)
            logger.error(MODULE_NAME + ".onAppStart could not initialize MongoDB connection");
        mongo.connect();

        // Find the first event document in the "events" collection that is marked for captioning
        try {
            String eventsColl = "events";
            Document eventDoc = mongo.getDatabase().getCollection(eventsColl)
                    .find(new Document("captions.nextEventToBeCaptioned", true)).first();
            if (eventDoc != null) {
                liveEventCollection = eventsColl;
                Document captionConfig = eventDoc.get("captions", Document.class);
                appInstance.getProperties().setProperty("added_stream_delay_in_ms", added_stream_delay_in_ms);
                if (captionConfig != null) {
                    enabled = captionConfig.getBoolean("enabled", enabled);
                    // map new naming: addedDelayForTranscriptionProcess -> added_stream_delay_in_ms
                    added_stream_delay_in_ms = captionConfig.getInteger("addedDelayForTranscriptionProcess", added_stream_delay_in_ms);

                    enabled_languages = captionConfig.getList("enabledLanguages", String.class);
                    if (enabled_languages != null && !enabled_languages.isEmpty()) {
                        String enabledCsv = String.join(",", enabled_languages);
                        appInstance.getProperties().setProperty("enabled_captions_csv", enabledCsv);
                        appInstance.getTimedTextProperties().setProperty(PROP_DEFAULT_CAPTION_LANGUAGES, enabledCsv);
                        logger.info(MODULE_NAME + ".onAppStart set enabled_captions_csv: " + enabledCsv);

                        showCaptionsInEvent = captionConfig.getBoolean("enabledForEventPage", showCaptionsInEvent);
                        try {
                            String eventInstance = "01";
                            // try to infer instance from event document stream block if present
                            try {
                                Document streamDoc = eventDoc.get("stream", Document.class);
                                if (streamDoc != null && streamDoc.containsKey("instance"))
                                    eventInstance = streamDoc.getString("instance");
                            } catch (Exception ignore) {}

                            for (String lang : enabled_languages) {
                                String smilName = customer + "_" + eventInstance + "_" + lang;
                                String baseSrc = eventInstance + "_" + lang + "_1080p";
                                String resp = SmilApiClient.createSmilForApplication(appInstance, smilName, baseSrc, enabled_languages, eventInstance, showCaptionsInEvent, lang);
                                logger.info(MODULE_NAME + ".onAppStart created SMIL: " + resp);
                            }
                        } catch (Exception e) {
                            logger.error(MODULE_NAME + ".onAppStart could not create SMIL", e);
                        }
                    }
                }
            }
        } catch (Exception e) {
            logger.error(MODULE_NAME + ".onAppStart MongoDB query failed", e);
        }

        if (!enabled)
            return;
        try
        {
            appInstance.addLiveStreamPacketizerListener(new LiveStreamPacketizerListener(appInstance));
                appInstance.addLiveStreamTranscoderListener(new CaptionsTranscoderCreateListener(new AzureCaptionsTranscoderActionListener(appInstance, speechHandlers, delayedStreams,
                    subscriptionKey, serviceRegion, mongo, liveEventCollection)));
            delayedStreamListener = new DelayedStreamListener(appInstance, delayedStreams);
            appInstance.addMediaCasterListener(delayedStreamListener);
        }
        catch (Exception e)
        {
            logger.error(MODULE_NAME + ".onAppStart exception", e);
        }
    }

    public void onStreamCreate(IMediaStream stream)
    {
        if (!enabled)
            return;
        stream.addClientListener(delayedStreamListener);
        stream.addLivePacketListener(delayedStreamListener);
    }
    public void onAppStop(IApplicationInstance appInstance) throws Exception
    {   
        //SmilApiClient.deleteSmilForApplication(appInstance);
        if (mongo != null) {
            mongo.disconnect();
            logger.info(MODULE_NAME + ".onAppStop MongoDB connection closed");
        }
        else {
            logger.info(MODULE_NAME + ".onAppStop MongoDB connection was not initialized");
        }
        logger.info(MODULE_NAME + ".onAppStop completed");

    }

}
