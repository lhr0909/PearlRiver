package com.hoolix.processor.sources;

import akka.stream.Attributes;
import akka.stream.Outlet;
import akka.stream.SourceShape;
import akka.stream.stage.AbstractOutHandler;
import akka.stream.stage.GraphStage;
import akka.stream.stage.GraphStageLogic;
import com.hoolix.processor.utils.KafkaConsumerUtil;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.ArrayDeque;
import java.util.Queue;
import java.util.Set;

/**
 * Hoolix 2017
 * Created by simon on 1/16/17.
 */
public class ReactiveKafkaSource extends GraphStage<SourceShape<ConsumerRecord<String, String>>> {

    public final Outlet<ConsumerRecord<String, String>> out = Outlet.create("KafkaRecordOut");
    private final SourceShape<ConsumerRecord<String, String>> shape = SourceShape.of(out);

    private final Set<String> kafkaTopics;

    public ReactiveKafkaSource(Set<String> topics) {
        kafkaTopics = topics;
    }

    @Override
    public SourceShape<ConsumerRecord<String, String>> shape() {
        return shape;
    }

    @Override
    public GraphStageLogic createLogic(Attributes inheritedAttributes) throws Exception {
        return new GraphStageLogic(shape()) {

            //TODO: This buffer is unlimited in size, watch out for OOM
            private Queue<ConsumerRecord<String, String>> buffer = new ArrayDeque<>();
            private final KafkaConsumer<String, String> kafkaConsumer = KafkaConsumerUtil.getInstance(kafkaTopics);

            @Override
            public void preStart() {
                pollIntoBuffer();
            }

            {
                setHandler(out, new AbstractOutHandler() {
                    @Override
                    public void onPull() throws Exception {
                        if (buffer.isEmpty()) {
                            pollIntoBuffer();
                        }

                        if (!buffer.isEmpty() && isAvailable(out)) {
                            push(out, buffer.poll());
                        }
                    }

                    //TODO: onDownstreamFinish()? I think we should just let the sink close the consumer on finish
                });
            }

            private void pollIntoBuffer() {
                //TODO: make timeout configurable?
                ConsumerRecords<String, String> records = kafkaConsumer.poll(0);
                for (ConsumerRecord<String, String> record : records) {
                    buffer.offer(record);
                }
            }
        };
    }

}
