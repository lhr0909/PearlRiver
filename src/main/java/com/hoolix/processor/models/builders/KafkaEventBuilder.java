package com.hoolix.processor.models.builders;

import akka.kafka.ConsumerMessage;
import com.hoolix.processor.models.Event;
import com.hoolix.processor.models.KafkaEvent;

/**
 * Hoolix 2017
 * Created by simon on 1/2/17.
 */
public class KafkaEventBuilder {
    private ConsumerMessage.CommittableOffset committableOffset;
    private Event event;

    public KafkaEventBuilder() {}

    public KafkaEventBuilder setEvent(Event event) {
        this.event = event;
        return this;
    }

    public KafkaEventBuilder setCommittableOffset(ConsumerMessage.CommittableOffset committableOffset) {
        this.committableOffset = committableOffset;
        return this;
    }

    public KafkaEvent build() {
        return new KafkaEvent(committableOffset, event);
    }
}
