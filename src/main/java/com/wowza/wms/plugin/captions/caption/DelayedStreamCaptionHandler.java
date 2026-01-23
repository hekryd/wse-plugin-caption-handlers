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

    private int wordsPerMinute = DEFAULT_WORDS_PER_MINUTE;

    public DelayedStreamCaptionHandler(IApplicationInstance appInstance, DelayedStream delayedStream, String streamName, Mongo mongo)
    {
        this.delayedStream = delayedStream;
        logger = WMSLoggerFactory.getLoggerObj(DelayedStreamCaptionHandler.class, appInstance);
        debugLog = appInstance.getProperties().getPropertyBoolean(PROP_CAPTIONS_DEBUG_LOG, false);
        this.streamName = streamName;
        this.mongo = mongo;
    }

    @Override
    public void handleCaption(Caption caption)
    {
        if (debugLog)
            logger.info(CLASS_NAME + ".handleCaption: caption = " + caption);
        if (delayedStream == null)
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
            logger.info(CLASS_NAME + ".handleCaption: packet = " + packet);
        delayedStream.writePacket(packet);

        // persist caption to Mongo (if available)
        if (mongo != null && mongo.getDatabase() != null) {
            try {
                Document doc = new Document()
                        .append("stream", streamName)
                        .append("language", caption.getLanguage())
                        .append("text", caption.getText())
                        .append("trackId", caption.getTrackId())
                        .append("startTime", Date.from(CaptionHelper.epochInstantFromMillis(caption.getBegin())))
                        .append("endTime", Date.from(CaptionHelper.epochInstantFromMillis(caption.getEnd())))
                        .append("createdAt", new Date());
                mongo.getDatabase().getCollection("caption").insertOne(doc);
                logger.info(CLASS_NAME + ".handleCaption: persisted caption to Mongo for stream " + streamName);
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
}
