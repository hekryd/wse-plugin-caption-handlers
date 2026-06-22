/*
 * This code and all components (c) Copyright 2006 - 2025, Wowza Media Systems, LLC.  All rights reserved.
 * This code is licensed pursuant to the Wowza Public License version 1.0, available at www.wowza.com/legal.
 */

package com.wowza.wms.plugin.captions.azure;

import com.microsoft.cognitiveservices.speech.*;
import com.microsoft.cognitiveservices.speech.audio.*;
import com.microsoft.cognitiveservices.speech.translation.*;
import com.wowza.wms.application.*;
import com.wowza.wms.logging.*;
import com.wowza.wms.plugin.captions.audio.SpeechHandler;
import com.wowza.wms.plugin.captions.caption.Caption;
import com.wowza.wms.plugin.captions.caption.CaptionHandler;
import com.wowza.wms.plugin.captions.caption.CaptionHelper;
import com.wowza.wms.plugin.captions.caption.CaptionTiming;
import com.wowza.wms.timedtext.model.ITimedTextConstants;
import com.wowza.wms.transcoder.model.TranscoderNativeAudioFrame;

import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static com.wowza.wms.plugin.captions.ModuleAzureSpeechToTextCaptions.*;
import static com.wowza.wms.plugin.captions.ModuleCaptionsBase.DEFAULT_FIRST_PASS_PERCENTAGE;
import static com.wowza.wms.plugin.captions.ModuleCaptionsBase.DEFAULT_FIRST_PASS_TERMINATORS;
import static com.wowza.wms.plugin.captions.ModuleCaptionsBase.MODULE_NAME;
import static com.wowza.wms.plugin.captions.ModuleCaptionsBase.PROP_CAPTIONS_DEBUG_LOG;
import static com.wowza.wms.plugin.captions.ModuleCaptionsBase.PROP_FIRST_PASS_PERCENTAGE;
import static com.wowza.wms.plugin.captions.ModuleCaptionsBase.PROP_LINE_TERMINATORS;
import static com.wowza.wms.plugin.captions.ModuleCaptionsBase.PROP_MAX_CAPTION_LINE_COUNT;
import static com.wowza.wms.plugin.captions.ModuleCaptionsBase.PROP_MAX_CAPTION_LINE_LENGTH;

public class AzureSpeechToTextHandler implements SpeechHandler
{
    private static final Class<AzureSpeechToTextHandler> CLASS = AzureSpeechToTextHandler.class;
    private static final String CLASS_NAME = CLASS.getSimpleName();
    public static final String DEFAULT_RECOGNITION_LANGUAGE = "en-US";

    // Property name for the recognition timeout, configurable per app instance
    private static final String PROP_RECOGNITION_TIMEOUT_MS = "azureRecognitionTimeoutMs";
    private static final long DEFAULT_RECOGNITION_TIMEOUT_MS = 5_000L;

    private final CaptionHandler captionHandler;
    private final WMSLogger logger;
    private final PushAudioInputStream audioStream = PushAudioInputStream.createPushStream();
    private final Semaphore semaphore = new Semaphore(0);
    private final SpeechConfig speechConfig;
    private final String recognitionLanguage;
    private final Map<String, String> languageMap;
    private final List<String> translationLanguages;
    private final List<String> phrases;
    private final boolean debugLog;
    private final int maxLineLength;
    private final int maxLines;
    private final String firstPassTerminators;
    private final int firstPassPercentage;
    private final long recognitionTimeoutMs;


    // Scheduler shared across all utterances in this session
    private final ScheduledExecutorService timeoutScheduler =
            Executors.newSingleThreadScheduledExecutor(r -> new Thread(r, "AzureRecognitionTimeout-" + Thread.currentThread().getName()));

    // Latest interim result, updated on every recognizing event
    private volatile String lastInterimText = null;
    private volatile Instant lastInterimStart = null;
    private volatile Instant lastInterimEnd = null;

    // The pending timeout task for the current utterance
    private volatile ScheduledFuture<?> pendingTimeout = null;

    // Set to true when the timeout fires and injects the interim caption,
    // so the eventual recognized event knows to skip itself
    private final AtomicBoolean timeoutFired = new AtomicBoolean(false);

    // Guards cancellation of an existing timeout when a new utterance starts
    private final Object timeoutLock = new Object();

    public AzureSpeechToTextHandler(IApplicationInstance appInstance, CaptionHandler captionHandler, String subscriptionKey,
            String serviceRegion, String streamName)
    {
        WMSProperties props = appInstance.getProperties();
        this.logger = WMSLoggerFactory.getLoggerObj(appInstance);
        debugLog = props.getPropertyBoolean(PROP_CAPTIONS_DEBUG_LOG, false);
        firstPassTerminators = props.getPropertyStr(PROP_LINE_TERMINATORS, DEFAULT_FIRST_PASS_TERMINATORS);
        firstPassPercentage = props.getPropertyInt(PROP_FIRST_PASS_PERCENTAGE, DEFAULT_FIRST_PASS_PERCENTAGE);
        maxLineLength = props.getPropertyInt(PROP_MAX_CAPTION_LINE_LENGTH, CaptionHelper.defaultMaxLineLengthSBCS);
        maxLines = props.getPropertyInt(PROP_MAX_CAPTION_LINE_COUNT, 2);
        recognitionTimeoutMs = appInstance.getProperties().getPropertyLong("delay_for_transcription_process", DEFAULT_RECOGNITION_TIMEOUT_MS);  
    
        this.captionHandler = captionHandler;

        String instanceName = streamName; // e.g., "01_de_1080p"
        String[] parts = instanceName.split("_");

        // default to en-US if nothing found
        String instanceLang = (parts.length > 1) ? parts[1].toLowerCase() : "en";

        // Language mapping - add more as needed
        Map<String, String> langMap = Map.of(
            "en", "en-US",
            "de", "de-DE",
            "fr", "fr-FR",
            "nl", "nl-NL",
            "es", "es-ES"
        );

        String bcp47Tag = langMap.getOrDefault(instanceLang, "en-US");
        recognitionLanguage = bcp47Tag;

        if (!isBCP47WithRegion(recognitionLanguage))
            throw new RuntimeException("Invalid recognition language: " + recognitionLanguage);

        String enabledCsv = appInstance.getProperties().getPropertyStr("enabled_captions_csv", null);
        String languagesStr = (enabledCsv == null || enabledCsv.isBlank())
                ? appInstance.getTimedTextProperties().getPropertyStr(PROP_DEFAULT_CAPTION_LANGUAGES, ITimedTextConstants.LANGUAGE_ID_ENGLISH)
                : enabledCsv;

        languageMap = Arrays.stream(languagesStr.split(","))
                .map(String::trim)
                .filter(s -> !s.isBlank())
                .collect(Collectors.toMap(
                        s -> toLocale(s).getLanguage(),
                        s -> s,
                        (existing, replacement) -> existing));

        translationLanguages = languageMap.keySet().stream()
                .filter(lang -> !lang.equals(Locale.forLanguageTag(recognitionLanguage).getLanguage()))
                .collect(Collectors.toList());

        String phraseStr = props.getPropertyStr(PROP_PHRASE_LIST, "");
        phrases = Arrays.stream(phraseStr.split(";"))
                .map(String::trim)
                .filter(s -> !s.isBlank())
                .collect(Collectors.toList());

        speechConfig = translationLanguages.isEmpty() ? SpeechConfig.fromSubscription(subscriptionKey, serviceRegion) :
                       SpeechTranslationConfig.fromSubscription(subscriptionKey, serviceRegion);
        speechConfig.setSpeechRecognitionLanguage(recognitionLanguage);

        ProfanityOption profanityOption = ProfanityOption.Masked;
        try
        {
            profanityOption = ProfanityOption.valueOf(props.getPropertyStr(PROP_PROFANITY_MASK_OPTION, "Masked"));
        }
        catch (IllegalArgumentException e)
        {
            // no-op
        }
        speechConfig.setProfanity(profanityOption);
    }

    @Override
    public void run()
    {
        try(speechConfig; audioStream; AudioConfig audioConfig = AudioConfig.fromStreamInput(audioStream))
        {
            Recognizer recognizer;
            if (speechConfig instanceof SpeechTranslationConfig) {
                translationLanguages.forEach(lang -> ((SpeechTranslationConfig) speechConfig).addTargetLanguage(lang));
                recognizer = new TranslationRecognizer((SpeechTranslationConfig) speechConfig, audioConfig);
            }
            else
                recognizer = new SpeechRecognizer(speechConfig, audioConfig);
            try (recognizer)
            {
                String recognizerName = recognizer.getClass().getSimpleName();

                if (!phrases.isEmpty())
                {
                    PhraseListGrammar grammar = PhraseListGrammar.fromRecognizer(recognizer);
                    phrases.forEach(grammar::addPhrase);
                }

                recognizer.sessionStarted.addEventListener((s, e) -> logger.info(MODULE_NAME + "::" + CLASS_NAME + "::" + recognizerName + " session started. session: " + e.getSessionId()));
                recognizer.sessionStopped.addEventListener(((s, e) -> logger.info(MODULE_NAME + "::" + CLASS_NAME + "::" + recognizerName + " session stopped. session: " + e.getSessionId())));
                recognizer.speechStartDetected.addEventListener((s, e) -> {
                    logger.info(MODULE_NAME + "::" + CLASS_NAME + "::" + recognizerName + " speech start detected. session: " + e.getSessionId());
                    scheduleUtteranceTimeout();
                });
                recognizer.speechEndDetected.addEventListener((s, e) -> logger.info(MODULE_NAME + "::" + CLASS_NAME + "::" + recognizerName + " speech End detected. session: " + e.getSessionId()));

                if (recognizer instanceof TranslationRecognizer)
                {
                    ((TranslationRecognizer)recognizer).recognizing.addEventListener((s, e) -> handleRecognizingEvent(e.getSessionId(), e.getResult()));
                    ((TranslationRecognizer)recognizer).recognized.addEventListener((s, e) -> handleRecognizedEvent(e.getSessionId(), e.getResult()));
                    ((TranslationRecognizer)recognizer).canceled.addEventListener((s, e) -> handleCancelledEvent(e.getSessionId(), e.getReason(), e.getErrorCode(), e.getErrorDetails()));
                    ((TranslationRecognizer)recognizer).startContinuousRecognitionAsync().get();
                    semaphore.acquire();
                    ((TranslationRecognizer)recognizer).stopContinuousRecognitionAsync().get();
                }
                else
                {
                    ((SpeechRecognizer)recognizer).recognizing.addEventListener((s, e) -> handleRecognizingEvent(e.getSessionId(), e.getResult()));
                    ((SpeechRecognizer)recognizer).recognized.addEventListener((s, e) -> handleRecognizedEvent(e.getSessionId(), e.getResult()));
                    ((SpeechRecognizer)recognizer).canceled.addEventListener((s, e) -> handleCancelledEvent(e.getSessionId(), e.getReason(), e.getErrorCode(), e.getErrorDetails()));

                    ((SpeechRecognizer)recognizer).startContinuousRecognitionAsync().get();
                    semaphore.acquire();
                    ((SpeechRecognizer)recognizer).stopContinuousRecognitionAsync().get();
                }
            }
            catch (InterruptedException ignored)
            {
            }
        }
        catch (Exception e)
        {
            logger.error(MODULE_NAME + "::" + CLASS_NAME + ".run exception",  e);
        }
        finally
        {
            // Always shut down the timeout scheduler when the session ends
            timeoutScheduler.shutdownNow();
        }
    }

    /**
     * Called when Azure detects speech start.
     * Cancels any previous timeout and schedules a fresh one for this utterance.
     * If the timeout fires before Azure sends a recognized event, we inject the
     * last interim caption and mark timeoutFired=true so the eventual recognized
     * event skips itself.
     */
    private void scheduleUtteranceTimeout()
    {
        synchronized (timeoutLock)
        {
            // Cancel any leftover timeout from a previous utterance
            if (pendingTimeout != null && !pendingTimeout.isDone())
                pendingTimeout.cancel(false);

            // Reset the flag for this new utterance
            timeoutFired.set(false);

            pendingTimeout = timeoutScheduler.schedule(() -> {
                // Only fire if recognized hasn't already handled this utterance
                if (!timeoutFired.compareAndSet(false, true))
                    return;

                String interimText = lastInterimText;
                Instant interimStart = lastInterimStart;
                Instant interimEnd = lastInterimEnd;

                if (interimText != null && !interimText.isBlank() && interimStart != null && interimEnd != null)
                {
                    logger.warn(MODULE_NAME + "::" + CLASS_NAME +
                        ".utteranceTimeout: Azure exceeded " + recognitionTimeoutMs +
                        "ms, injecting interim caption: \"" + interimText + "\"");

                    CaptionTiming timing = new CaptionTiming(interimStart, interimEnd);
                    List<Caption> captions = CaptionHelper.getCaptions(
                        languageMap.get(Locale.forLanguageTag(recognitionLanguage).getLanguage()),
                        maxLineLength, maxLines, firstPassTerminators, firstPassPercentage,
                        timing, interimText);
                    captions.forEach(captionHandler::handleCaption);
                }
                else
                {
                    logger.warn(MODULE_NAME + "::" + CLASS_NAME +
                        ".utteranceTimeout: Azure exceeded " + recognitionTimeoutMs +
                        "ms and no interim caption was available — dropping utterance.");
                }

                // Clear interim state so stale data doesn't bleed into the next utterance
                lastInterimText = null;
                lastInterimStart = null;
                lastInterimEnd = null;

            }, recognitionTimeoutMs, TimeUnit.MILLISECONDS);
        }
    }

    private void handleRecognizingEvent(String sessionId, RecognitionResult result)
    {
        // Always cache the latest partial result regardless of debugLog,
        // so the timeout task always has something to fall back to
        lastInterimText = result.getText();
        lastInterimStart = CaptionHelper.epochInstantFromTicks(result.getOffset());
        lastInterimEnd = CaptionHelper.epochInstantFromTicks(result.getOffset().add(result.getDuration()));

        // Reset the timeout on every interim result
        scheduleUtteranceTimeout();

        if (debugLog)
        {
            long latency = Long.parseLong(result.getProperties().getProperty(PropertyId.SpeechServiceResponse_RecognitionLatencyMs));
            String json = result.getProperties().getProperty(PropertyId.SpeechServiceResponse_JsonResult);
            logger.info(MODULE_NAME + "::" + CLASS_NAME + "handleRecognizingEvent: session: " + sessionId +
                " RECOGNIZING: Timing: " + getTimestamp(lastInterimStart, lastInterimEnd) +
                " Latency=" + latency + " Result=" + json);
        }
    }

    private void handleRecognizedEvent(String sessionId, RecognitionResult result)
    {
        // Cancel the timeout — Azure responded in time
        synchronized (timeoutLock)
        {
            if (pendingTimeout != null && !pendingTimeout.isDone())
                pendingTimeout.cancel(false);
        }

        // If the timeout already fired and injected an interim caption, skip this result
        if (timeoutFired.getAndSet(false))
        {
            if (true)
                logger.info(MODULE_NAME + "::" + CLASS_NAME +
                    "handleRecognizedEvent: session: " + sessionId +
                    " skipping recognized result — timeout already injected interim caption.");
            lastInterimText = null;
            lastInterimStart = null;
            lastInterimEnd = null;
            return;
        }

        // Clear interim state — recognized result takes over
        lastInterimText = null;
        lastInterimStart = null;
        lastInterimEnd = null;

        if (result.getReason() == ResultReason.NoMatch && debugLog)
            logger.info(MODULE_NAME + "::" + CLASS_NAME + "handleRecognizedEvent: session: " + sessionId + " NOMATCH: Speech could not be recognized.");
        else
        {
            Instant start = CaptionHelper.epochInstantFromTicks(result.getOffset());
            Instant end = CaptionHelper.epochInstantFromTicks(result.getOffset().add(result.getDuration()));
            long latency = Long.parseLong(result.getProperties().getProperty(PropertyId.SpeechServiceResponse_RecognitionLatencyMs));
            String json = result.getProperties().getProperty(PropertyId.SpeechServiceResponse_JsonResult);
            if (debugLog)
                logger.info(MODULE_NAME + "::" + CLASS_NAME + "handleRecognizedEvent: session: " + sessionId +
                    " RECOGNIZED: Timing: " + getTimestamp(start, end) + " Latency=" + latency + " Result=" + json);
            handleResult(result, start, end);
        }
    }

    private void handleResult(RecognitionResult result, Instant start, Instant end)
    {
        CaptionTiming captionTiming = new CaptionTiming(start, end);
        List<Caption> sourceCaptions = CaptionHelper.getCaptions(languageMap.get(Locale.forLanguageTag(recognitionLanguage).getLanguage()), maxLineLength, maxLines,
				firstPassTerminators, firstPassPercentage, captionTiming, result.getText());
        sourceCaptions.forEach(captionHandler::handleCaption);

        if (result instanceof TranslationRecognitionResult)
        {
            ((TranslationRecognitionResult)result).getTranslations().forEach((language, translation) -> {
                List<Caption> translatedCaptions = CaptionHelper.getCaptions(languageMap.get(language), maxLineLength, maxLines,
                        firstPassTerminators, firstPassPercentage, captionTiming, translation);
                translatedCaptions.forEach(captionHandler::handleCaption);
            });
        }
    }

    private void handleCancelledEvent(String sessionId, CancellationReason reason, CancellationErrorCode errorCode, String errorDetails)
    {
        logger.warn(MODULE_NAME + "::" + CLASS_NAME + "handleCancelledEvent: session: " + sessionId + " Translation Session Cancelled: Reason=" + reason);
        if (reason == CancellationReason.Error) {
            logger.error(MODULE_NAME + "::" + CLASS_NAME + "handleCancelledEvent: session: " + sessionId + " Translation Session Cancelled: ErrorCode=" + errorCode);
            logger.error(MODULE_NAME + "::" + CLASS_NAME + "handleCancelledEvent: session: " + sessionId + " Translation Session Cancelled: ErrorDetails=" + errorDetails);
        }
        semaphore.release();
    }

    @Override
    public void addAudioFrame(TranscoderNativeAudioFrame frame)
    {
        try {
            audioStream.write(frame.buffer);
        }
        catch (IllegalStateException ise)
        {
            if (debugLog)
                logger.warn(MODULE_NAME + "::" + CLASS_NAME + " addAudioFrame: audioStream closed, ignoring frame");
        }
        catch (Exception e)
        {
            logger.error(MODULE_NAME + "::" + CLASS_NAME + " addAudioFrame exception", e);
        }
    }

    @Override
    public void close()
    {
        audioStream.close();
        semaphore.release();
    }

    private String getTimestamp(Instant startTime, Instant endTime)
    {
        var format = "HH:mm:ss.SSS";
        var formatter = DateTimeFormatter.ofPattern(format).withZone(ZoneId.from(ZoneOffset.UTC));
        return String.format("%s --> %s", formatter.format(startTime), formatter.format(endTime));
    }
}