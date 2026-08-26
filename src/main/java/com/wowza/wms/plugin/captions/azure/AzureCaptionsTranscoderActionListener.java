/*
 * This code and all components (c) Copyright 2006 - 2025, Wowza Media Systems, LLC.  All rights reserved.
 * This code is licensed pursuant to the Wowza Public License version 1.0, available at www.wowza.com/legal.
 */

package com.wowza.wms.plugin.captions.azure;

import com.wowza.wms.plugin.captions.audio.SpeechHandler;
import com.wowza.wms.plugin.captions.caption.CaptionHandler;
import com.wowza.wms.plugin.captions.stream.DelayedStream;
import com.wowza.wms.application.IApplicationInstance;
import com.wowza.wms.plugin.captions.transcoder.AudioResamplingTranscoderActionListener;
import org.bson.Document;

import java.util.Map;
import java.util.function.Consumer;

public class AzureCaptionsTranscoderActionListener extends AudioResamplingTranscoderActionListener
{
    private final String subscriptionKey;
    private final String serviceRegion;
    private final Consumer<Document> eventSelected;

    public AzureCaptionsTranscoderActionListener(IApplicationInstance appInstance, Map<String, SpeechHandler> handlers, Map<String, DelayedStream> delayedStreams,
                                                 String subscriptionKey, String serviceRegion, com.wowza.wms.plugin.captions.mongo.Mongo mongo, String eventCollection, String eventKey,
                                                 Long eventStartAtMillis, Consumer<Document> eventSelected)
    {
        super(appInstance, handlers, delayedStreams, mongo, eventCollection, eventKey, eventStartAtMillis);
        this.subscriptionKey = subscriptionKey;
        this.serviceRegion = serviceRegion;
        this.eventSelected = eventSelected;
    }

    @Override
    public SpeechHandler getSpeechHandler(CaptionHandler captionHandler,String streamName)
    {
        return new AzureSpeechToTextHandler(appInstance, captionHandler, subscriptionKey, serviceRegion,streamName);
    }

    @Override
    protected void onEventSelected(Document eventDoc)
    {
        if (eventSelected != null)
            eventSelected.accept(eventDoc);
    }
}
