package io.smallrye.reactive.messaging.kafka.commit;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import io.smallrye.reactive.messaging.kafka.commit.KafkaCheckpointCommit.CheckpointState;

class CheckpointStateTest {

    private final TopicPartition tp = new TopicPartition("topic", 0);

    @Test
    void testEmptyStateIsNotUnsynced() {
        CheckpointState<Integer> state = new CheckpointState<>(tp);

        assertThat(state.hasUnsyncedOffset()).isFalse();
        assertThat(state.millisSinceLastPersistedOffset()).isEqualTo(-1);
    }

    @Test
    void testFetchedStateAgeCountsFromCreation() {
        CheckpointState<Integer> state = new CheckpointState<>(tp, new ProcessingState<>(42, 81));

        assertThat(state.hasUnsyncedOffset()).isTrue();
        assertThat(state.millisSinceLastPersistedOffset()).isBetween(0L, 10_000L);
    }

    @Test
    void testReceivedRecordAgeCountsFromCreation() {
        CheckpointState<Integer> state = new CheckpointState<>(tp);
        state.receivedRecord();

        assertThat(state.millisSinceLastPersistedOffset()).isBetween(0L, 10_000L);
    }
}
