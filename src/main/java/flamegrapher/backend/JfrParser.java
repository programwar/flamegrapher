package flamegrapher.backend;

import flamegrapher.jfrextractors.ItemAttributeExtractor;
import flamegrapher.model.RecordingDuration;
import org.openjdk.jmc.common.item.*;
import org.openjdk.jmc.common.unit.*;
import org.openjdk.jmc.flightrecorder.JfrAttributes;

import java.io.File;
import java.io.IOException;
import java.util.Stack;
import java.util.function.Supplier;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.openjdk.jmc.common.IMCFrame;
import org.openjdk.jmc.common.IMCStackTrace;
import org.openjdk.jmc.flightrecorder.CouldNotLoadRecordingException;
import org.openjdk.jmc.flightrecorder.JfrLoaderToolkit;
import org.openjdk.jmc.flightrecorder.jdk.JdkAttributes;
import org.openjdk.jmc.flightrecorder.jdk.JdkTypeIDs;

import flamegrapher.backend.JsonOutputWriter.StackFrame;
import org.openjdk.jmc.flightrecorder.stacktrace.FrameSeparator;
import org.openjdk.jmc.flightrecorder.stacktrace.StacktraceFormatToolkit;

public class JfrParser {

    public StackFrame toJson(File jfr, String... eventTypes) throws IOException, CouldNotLoadRecordingException {

        IItemCollection filtered = JfrLoaderToolkit.loadEvents(jfr)
            .apply(ItemFilters.type(eventTypes));
                //.apply(ItemFilters.rangeContainedIn(JfrAttributes.LIFETIME, range));

        JsonOutputWriter writer = new JsonOutputWriter();
        filtered.forEach(events -> {
            // events is an IItemIterable array of events sharing a single type.
            IMemberAccessor<IMCStackTrace, IItem> accessor = events.getType()
                .getAccessor(JfrAttributes.EVENT_STACKTRACE.getKey());

            for (IItem item : events) {
                Stack<String> stack = new Stack<>();
                IMCStackTrace stackTrace = accessor.getMember(item);
                if (stackTrace == null || stackTrace.getFrames() == null) {
                    continue;
                }
                stackTrace.getFrames()
                    .forEach(frame -> {
                        stack.push(getFrameName(frame));
                    });
                Long value = getValue(events, item, eventTypes);
                writer.processEvent(stack, value);
            }
        });

        return writer.getStackFrame();
    }


    /**
     * Returns the value according to the event type. For most event types, we
     * will only take into account the occurrence in itself. For example, Method
     * CPU sample will only return 1, i.e. one occurence of that given stack
     * trace while sampling. But for locks, we will look into the total time
     * spent waiting for that lock. For allocation, we will look at the total
     * amount of memory allocated. And so on.
     *
     * @param events
     */
    private Long getValue(IItemIterable events, IItem item, String[] eventTypes) {

        for (String eventType : eventTypes) {
            if (JdkTypeIDs.MONITOR_ENTER.equals(eventType)) {
                return getOrCalculateDuration(item, events);

            } else if (JdkTypeIDs.ALLOC_INSIDE_TLAB.equals(eventType)
                    || JdkTypeIDs.ALLOC_OUTSIDE_TLAB.equals(eventType)) {
                IMemberAccessor<IQuantity, IItem> accessor = events.getType()
                                                                   .getAccessor(JdkAttributes.ALLOCATION_SIZE.getKey());
                IQuantity allocationSize = accessor.getMember(item);
                return allocationSize.clampedLongValueIn(UnitLookup.BYTE);
            }
        }
        // For all other event types, simply return 1.
        return 1L;
    }

    /**
     * Calculates duration using start and end times when duration accessor is missing.
     *
     * @param item
     * @param events
     * @return
     */
    private Long getOrCalculateDuration(IItem item, IItemIterable events) {
        Long duration;

        final IMemberAccessor<IQuantity, IItem> durationAccessor = events.getType()
                                                                         .getAccessor(JfrAttributes.DURATION.getKey());

        if (durationAccessor != null) {
            duration = durationAccessor.getMember(item).clampedLongValueIn(UnitLookup.MILLISECOND);

        } else {
            final IMemberAccessor<IQuantity, IItem> startAccessor = events.getType()
                                                                          .getAccessor(JfrAttributes.START_TIME.getKey());
            final IMemberAccessor<IQuantity, IItem> endAccessor = events.getType()
                                                                          .getAccessor(JfrAttributes.END_TIME.getKey());

            if (startAccessor != null && endAccessor != null) {
                duration = endAccessor.getMember(item).subtract(startAccessor.getMember(item))
                                                      .clampedLongValueIn(UnitLookup.MILLISECOND);
            } else {
                throw new ParserException("Event duration is unknown!");
            }
        }

        return duration;
    }

    private String getFrameName(IMCFrame frame) {
        return StacktraceFormatToolkit.formatFrame(
                frame,
                new FrameSeparator(FrameSeparator.FrameCategorization.LINE, false),
                false,
                false,
                true,
                true,
                false,
                true
        );
    }

    public RecordingDuration getJfrDuration(File jfr, String[] eventTypes) throws IOException, CouldNotLoadRecordingException {
        /*
        String startDate = "2019-10-23 11:45:10";
        String endDate = "2019-10-23 11:45:11";
        LocalDateTime localDateTime = LocalDateTime.parse(startDate,
                DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss") );

        long startMillis = localDateTime
                .atZone(ZoneId.systemDefault())
                .toInstant().toEpochMilli();

        localDateTime = LocalDateTime.parse(endDate,
                DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss") );

        long endMililis = localDateTime
                .atZone(ZoneId.systemDefault())
                .toInstant().toEpochMilli();


        IQuantity start = UnitLookup.EPOCH_MS.quantity(startMillis);
        IQuantity end = UnitLookup.EPOCH_MS.quantity(endMililis);
        IRange<IQuantity> range = QuantityRange.createWithEnd(start, end);
         */

        RecordingDuration duration = new RecordingDuration();

        IItemCollection filtered = JfrLoaderToolkit.loadEvents(jfr)
                .apply(ItemFilters.type(eventTypes));

        Supplier<Stream> stream = () -> StreamSupport.stream(filtered.spliterator(), false)
                                                .flatMap(eventStream -> StreamSupport.stream(eventStream.spliterator(), false));

        var firstEvent = (IItem)stream.get().findFirst().get();
        var lastEvent = (IItem)stream.get().skip(stream.get().count() - 1).findFirst().get();

        duration.setStartMilliseconds(ItemAttributeExtractor.getEventStartTimeMs(firstEvent));
        duration.setEndMilliseconcs(ItemAttributeExtractor.getEventStartTimeMs(lastEvent));

        /*IType<IItem> itemType = ItemToolkit.getItemType(firstEvent);
        duration.setStartMilliseconds(itemType.getAccessor(JfrAttributes.START_TIME.getKey()).getMember(firstEvent)
                                                .in(UnitLookup.EPOCH_MS).longValue());

        itemType = ItemToolkit.getItemType(lastEvent);
        duration.setEndMilliseconcs(itemType.getAccessor(JfrAttributes.START_TIME.getKey()).getMember(lastEvent)
                                            .in(UnitLookup.EPOCH_MS).longValue());*/

        return duration;
    }
}
