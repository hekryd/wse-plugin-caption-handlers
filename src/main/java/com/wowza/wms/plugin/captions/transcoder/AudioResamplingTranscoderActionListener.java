/*
 * This code and all components (c) Copyright 2006 - 2025, Wowza Media Systems, LLC.  All rights reserved.
 * This code is licensed pursuant to the Wowza Public License version 1.0, available at www.wowza.com/legal.
 */

package com.wowza.wms.plugin.captions.transcoder;

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

    public AudioResamplingTranscoderActionListener(IApplicationInstance appInstance, Map<String, SpeechHandler> handlers, Map<String, DelayedStream> delayedStreams)
    {
        this.appInstance = appInstance;
        this.handlers = handlers;
        this.delayedStreams = delayedStreams;
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
        TranscoderSessionAudio sessionAudio = transcoder.getTranscodingSession().getSessionAudio();
        SpeechHandler speechHandler = handlers.computeIfAbsent(mappedName, k -> {
            DelayedStream delayedStream = delayedStreams.computeIfAbsent(mappedName,
                    name -> new DelayedStream(appInstance, streamName, Executors.newSingleThreadScheduledExecutor()));
            CaptionHandler captionHandler = new DelayedStreamCaptionHandler(appInstance, delayedStream);
            SpeechHandler handler = getSpeechHandler(captionHandler ,streamName);
            new Thread(handler, AzureSpeechToTextHandler.class.getSimpleName() + "[" + appInstance.getContextStr() + "/" + streamName + "]")
                    .start();
            return handler;
        });
        TranscoderAudioFrameListener frameListener = new TranscoderAudioFrameListener(speechHandler);
        sessionAudio.addFrameListener(frameListener);
    }

    public abstract SpeechHandler getSpeechHandler(CaptionHandler captionHandler,String streamName);

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
