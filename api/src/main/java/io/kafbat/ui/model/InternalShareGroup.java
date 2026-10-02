package io.kafbat.ui.model;

import static io.kafbat.ui.util.ConsumerGroupUtil.calculateConsumerLag;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.Builder;
import lombok.Data;
import org.apache.kafka.clients.admin.ShareGroupDescription;
import org.apache.kafka.common.GroupState;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;

@Data
@Builder(toBuilder = true)
public class InternalShareGroup {
  private final String groupId;
  private final boolean simple;
  private final Collection<InternalMember> members;
  private final Map<TopicPartition, Long> offsets;
  private final Map<TopicPartition, Long> endOffsets;
  private final Long consumerLag;
  private final Integer topicNum;
  private final String partitionAssignor;
  private final GroupState state;
  private final Node coordinator;

  @Data
  @Builder(toBuilder = true)
  public static class InternalMember {
    private final String consumerId;
    private final String groupInstanceId;
    private final String clientId;
    private final String host;
    private final Set<TopicPartition> assignment;
  }

  public static InternalShareGroup create(
      ShareGroupDescription description,
      Map<TopicPartition, Long> groupOffsets,
      Map<TopicPartition, Long> topicEndOffsets) {
    var builder = InternalShareGroup.builder();
    builder.groupId(description.groupId());
    // builder.simple(description.c());
    builder.state(description.groupState());
    // builder.partitionAssignor(description.partitionAssignor());
    Collection<InternalMember> internalMembers = initInternalMembers(description);
    builder.members(internalMembers);
    builder.offsets(groupOffsets);
    builder.endOffsets(topicEndOffsets);
    builder.consumerLag(calculateConsumerLag(groupOffsets, topicEndOffsets));
    builder.topicNum(calculateTopicNum(groupOffsets, internalMembers));
    Optional.ofNullable(description.coordinator()).ifPresent(builder::coordinator);
    return builder.build();
  }

  private static Integer calculateTopicNum(Map<TopicPartition, Long> offsets, Collection<InternalMember> members) {

    return (int) Stream.concat(
        offsets.keySet().stream().map(TopicPartition::topic),
        members.stream()
            .flatMap(m -> m.getAssignment().stream().map(TopicPartition::topic))
    ).distinct().count();

  }

  private static Collection<InternalMember> initInternalMembers(ShareGroupDescription description) {
    return description.members().stream()
        .map(m ->
            InternalShareGroup.InternalMember.builder()
                .assignment(m.assignment().topicPartitions())
                .clientId(m.clientId())
                .groupInstanceId(m.consumerId())
                .consumerId(m.consumerId())
                .clientId(m.clientId())
                .host(m.host())
                .build()
        ).collect(Collectors.toList());
  }


}
