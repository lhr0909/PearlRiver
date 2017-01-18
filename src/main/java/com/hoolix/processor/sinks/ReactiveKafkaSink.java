package com.hoolix.processor.sinks;

import akka.Done;
import akka.dispatch.Futures;
import akka.stream.Attributes;
import akka.stream.Inlet;
import akka.stream.SinkShape;
import akka.stream.stage.AbstractInHandler;
import akka.stream.stage.GraphStageLogic;
import akka.stream.stage.GraphStageWithMaterializedValue;
import com.hoolix.processor.utils.KafkaConsumerUtil;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import scala.Tuple2;
import scala.concurrent.Future;
import scala.concurrent.Promise;

import java.util.Map;
import java.util.Set;

/**
 * Hoolix 2017
 * Created by simon on 1/17/17.
 */
public class ReactiveKafkaSink extends GraphStageWithMaterializedValue<SinkShape<Map<TopicPartition, OffsetAndMetadata>>, Future<Done>> {
    public final Inlet<Map<TopicPartition, OffsetAndMetadata>> in = Inlet.create("KafkaOffsetIn");
    private final SinkShape<Map<TopicPartition, OffsetAndMetadata>> shape = SinkShape.of(in);

    private final Set<String> kafkaTopics;

    public ReactiveKafkaSink(Set<String> topics) {
        kafkaTopics = topics;
    }

    @Override
    public SinkShape<Map<TopicPartition, OffsetAndMetadata>> shape() {
        return shape;
    }

    @Override
    public Tuple2<GraphStageLogic, Future<Done>> createLogicAndMaterializedValue(Attributes inheritedAttributes) throws Exception {
        Promise<Done> promise = Futures.promise();

        GraphStageLogic graphStageLogic = new GraphStageLogic(shape()) {

            private final KafkaConsumer<String, String> kafkaConsumer = KafkaConsumerUtil.getInstance(kafkaTopics);

            @Override
            public void preStart() throws Exception {
                pull(in);
            }

            {
                setHandler(in, new AbstractInHandler() {
                    @Override
                    public void onPush() throws Exception {
                        if (isAvailable(in)) {
                            Map<TopicPartition, OffsetAndMetadata> offsets = grab(in);
                            kafkaConsumer.commitSync(offsets);

                            if (!hasBeenPulled(in)) {
                                pull(in);
                            }
                        }
                    }

                    @Override
                    public void onUpstreamFinish() throws Exception {
                        KafkaConsumerUtil.closeConsumer(kafkaTopics);
                        promise.success(Done.getInstance());
                        super.onUpstreamFinish();
                    }
                });
            }
        };

        return new Tuple2<>(graphStageLogic, promise.future());
    }
}
