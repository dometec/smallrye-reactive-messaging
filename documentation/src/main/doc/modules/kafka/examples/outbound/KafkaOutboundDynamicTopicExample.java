package outbound;

import io.smallrye.reactive.messaging.kafka.OutgoingKafkaRecordMetadata;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.eclipse.microprofile.reactive.messaging.Message;

public class KafkaOutboundDynamicTopicExample {

    public Message<Double> metadata(Message<Double> incoming) {
        // tag::code[]
        String topicName = selectTopicFromIncommingMessage(incoming);
        OutgoingKafkaRecordMetadata<String> metadata = OutgoingKafkaRecordMetadata.<String>builder()
            .withTopic(topicName)
            .build();

        // Create a new message from the `incoming` message
        // Add `metadata` to the metadata from the `incoming` message.
        return incoming.addMetadata(metadata);
        // end::code[]
    }

    private String selectTopicFromIncommingMessage(Message<Double> incoming) {
        return "fake";
    }
}
