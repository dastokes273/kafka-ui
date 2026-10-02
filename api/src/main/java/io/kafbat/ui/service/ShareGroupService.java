package io.kafbat.ui.service;

import com.google.common.collect.Table;
import io.kafbat.ui.config.ClustersProperties;
import io.kafbat.ui.model.InternalConsumerGroup;
import io.kafbat.ui.model.InternalShareGroup;
import io.kafbat.ui.model.KafkaCluster;
import io.kafbat.ui.service.rbac.AccessControlService;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.ShareGroupDescription;
import org.apache.kafka.common.TopicPartition;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

@Service
@RequiredArgsConstructor
public class ShareGroupService {

  private final AdminClientService adminClientService;
  private final AccessControlService accessControlService;
  private final ClustersProperties clustersProperties;
  private final StatisticsCache statisticsCache;

  private Mono<List<InternalShareGroup>> getConsumerShareGroups(
      ReactiveAdminClient ac,
      List<ShareGroupDescription> descriptions) {
    var groupNames = descriptions.stream().map(ShareGroupDescription::groupId).toList();
    // 1. getting committed offsets for all groups
    return ac.listShareGroupOffsets(groupNames, null)
        .flatMap((Table<String, TopicPartition, Long> committedOffsets) -> {
          // 2. getting end offsets for partitions with committed offsets
          return ac.listOffsets(committedOffsets.columnKeySet(), OffsetSpec.latest(), false)
              .map(endOffsets ->
                  descriptions.stream()
                      .map(desc -> {
                        var groupOffsets = committedOffsets.row(desc.groupId());
                        var endOffsetsForGroup = new HashMap<>(endOffsets);
                        endOffsetsForGroup.keySet().retainAll(groupOffsets.keySet());
                        // 3. gathering description & offsets
                        return InternalConsumerGroup.create(desc, groupOffsets, endOffsetsForGroup);
                      })
                      .collect(Collectors.toList()));
        });
  }

  public Mono<InternalShareGroup> getConsumerShareGroupDetail(KafkaCluster cluster,
                                                              String consumerGroupId) {
    return adminClientService.get(cluster)
        .flatMap(ac -> ac.describeShareGroups(List.of(consumerGroupId))
            .filter(m -> m.containsKey(consumerGroupId))
            .map(r -> r.get(consumerGroupId))
            .flatMap(descr ->
                getConsumerShareGroups(ac, List.of(descr))
                    .filter(groups -> !groups.isEmpty())
                    .map(groups -> groups.get(0))));
  }
}
