package io.kafbat.ui.service;

import com.google.common.collect.Streams;
import com.google.common.collect.Table;
import io.kafbat.ui.config.ClustersProperties;
import io.kafbat.ui.model.InternalShareGroup;
import io.kafbat.ui.model.KafkaCluster;
import io.kafbat.ui.model.ServerStatusDTO;
import io.kafbat.ui.model.ShareGroupOrderingDTO;
import io.kafbat.ui.model.ShareGroupStateDTO;
import io.kafbat.ui.model.SortOrderDTO;
import io.kafbat.ui.model.Statistics;
import io.kafbat.ui.service.index.GroupFilter;
import io.kafbat.ui.service.metrics.scrape.ScrapedClusterState;
import io.kafbat.ui.service.rbac.AccessControlService;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.function.ToIntFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.admin.GroupListing;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.ShareGroupDescription;
import org.apache.kafka.common.GroupState;
import org.apache.kafka.common.TopicPartition;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

@Service
@RequiredArgsConstructor
public class ShareGroupService {

  private final AdminClientService adminClientService;
  private final AccessControlService accessControlService;
  private final ClustersProperties clustersProperties;
  private final StatisticsCache statisticsCache;

  public Mono<InternalShareGroup> getShareGroupDetail(KafkaCluster cluster,
                                                      String consumerGroupId) {
    return adminClientService.get(cluster)
        .flatMap(ac -> ac.describeShareGroups(List.of(consumerGroupId))
            .filter(m -> m.containsKey(consumerGroupId))
            .map(r -> r.get(consumerGroupId))
            .flatMap(descr ->
                getShareGroups(ac, List.of(descr))
                    .filter(groups -> !groups.isEmpty())
                    .map(groups -> groups.get(0))));
  }

  private Mono<List<InternalShareGroup>> getShareGroups(
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
                        return InternalShareGroup.create(desc, groupOffsets, endOffsetsForGroup);
                      })
                      .collect(Collectors.toList()));
        });
  }

  private Mono<List<InternalShareGroup>> getShareGroups(KafkaCluster cluster,
                                                              ReactiveAdminClient ac,
                                                              List<ShareGroupDescription> descriptions) {

    Statistics statistics = statisticsCache.get(cluster);
    if (!statistics.getStatus().equals(ServerStatusDTO.ONLINE)) {
      return getShareGroups(ac, descriptions);
    }

    Map<String, InternalShareGroup> result = new HashMap<>();

    var cachedConsumerGroupsStates = statistics.getClusterState().getConsumerGroupsStates();
    var cachedTopicStates = statistics.getClusterState().getTopicStates();
    var missed = new ArrayList<ShareGroupDescription>();

    for (ShareGroupDescription consumerGroup : descriptions) {
      Optional<InternalShareGroup> internalShareGroup =
          getShareGroup(consumerGroup, cachedConsumerGroupsStates, cachedTopicStates);
      if (internalShareGroup.isPresent()) {
        result.put(consumerGroup.groupId(), internalShareGroup.get());
      } else {
        missed.add(consumerGroup);
      }
    }

    Mono<Map<String, InternalShareGroup>> shareGroups = Mono.just(result);
    if (!missed.isEmpty()) {
      shareGroups = getShareGroups(ac, missed).map(r -> {
            var combined = new HashMap<>(result);
            combined.putAll(r.stream().collect(Collectors.toMap(
                InternalShareGroup::getGroupId,
                d -> d
            )));
            return combined;
          }
      );
    }

    return shareGroups.map(res ->
        descriptions.stream().map(d -> res.get(d.groupId())).toList()
    );
  }

  public Mono<ShareGroupsPage> getShareGroups(
      KafkaCluster cluster,
      OptionalInt pageNum,
      OptionalInt perPage,
      @Nullable String search,
      Boolean fts,
      ShareGroupOrderingDTO orderBy,
      SortOrderDTO sortOrderDto,
      List<ShareGroupStateDTO> states) {
    return adminClientService.get(cluster).flatMap(ac ->
        ac.listShareGroups()
            .map(listing -> filterGroups(listing, search, fts))
            .map(listing -> filterByState(listing, states))
            .flatMapIterable(lst -> lst)
            .filterWhen(cg -> accessControlService.isConsumerGroupAccessible(cg.groupId(), cluster.getName()))
            .collectList()
            .flatMap(allGroups ->
                loadSortedDescriptions(cluster, ac, allGroups, pageNum, perPage, orderBy, sortOrderDto)
                    .flatMap(descriptions -> getShareGroups(cluster, ac, descriptions)
                        .map(page ->
                            ShareGroupsPage.from(page, allGroups.size(), pageNum, perPage)
                        )
                    )
            )
    );
  }

  private Optional<InternalShareGroup> getShareGroup(
      ShareGroupDescription consumerGroup,
      Map<String, ScrapedClusterState.ConsumerGroupState> cachedConsumerGroupsStates,
      Map<String, ScrapedClusterState.TopicState> cachedTopicStates) {
    var consumerGroupState = cachedConsumerGroupsStates.get(consumerGroup.groupId());
    if (consumerGroupState != null) {
      Map<TopicPartition, Long> groupOffsets = consumerGroupState.committedOffsets();
      Map<TopicPartition, Long> endOffsets = new HashMap<>();
      boolean cacheComplete = true;

      for (TopicPartition topicPartition : groupOffsets.keySet()) {
        var topicState = cachedTopicStates.get(topicPartition.topic());
        if (topicState == null || !topicState.endOffsets().containsKey(topicPartition.partition())) {
          cacheComplete = false;
          break;
        }
        endOffsets.put(topicPartition, topicState.endOffsets().get(topicPartition.partition()));
      }

      if (cacheComplete) {
        return Optional.of(
            InternalShareGroup.create(consumerGroup, groupOffsets, endOffsets)
        );
      }
    }

    return Optional.empty();
  }

  private Collection<GroupListing> filterGroups(Collection<GroupListing> groups, String search,
                                                Boolean useFts) {
    ClustersProperties.ClusterFtsProperties ftsProperties = clustersProperties.getFts();
    boolean fts = ftsProperties.use(useFts);
    GroupFilter filter = new GroupFilter(groups, fts, ftsProperties.getConsumers());
    return filter.find(search);
  }

  private Collection<GroupListing> filterByState(Collection<GroupListing> groups,
                                                         List<ShareGroupStateDTO> states) {
    if (states.isEmpty()) {
      return groups;
    }
    Set<GroupState> kafkaStates = states.stream()
        .map(this::mapToKafkaState)
        .collect(Collectors.toSet());
    return groups.stream()
        .filter(cg -> kafkaStates.contains(cg.groupState().orElse(GroupState.UNKNOWN)))
        .toList();
  }

  private GroupState mapToKafkaState(ShareGroupStateDTO stateDto) {
    return switch (stateDto) {
      case UNKNOWN -> GroupState.UNKNOWN;
      case PREPARING_REBALANCE -> GroupState.PREPARING_REBALANCE;
      case COMPLETING_REBALANCE -> GroupState.COMPLETING_REBALANCE;
      case STABLE -> GroupState.STABLE;
      case DEAD -> GroupState.DEAD;
      case EMPTY -> GroupState.EMPTY;
    };
  }

  private Mono<List<ShareGroupDescription>> loadSortedDescriptions(KafkaCluster cluster,
                                                                      ReactiveAdminClient ac,
                                                                      List<GroupListing> groups,
                                                                      OptionalInt pageNum,
                                                                      OptionalInt perPage,
                                                                      ShareGroupOrderingDTO orderBy,
                                                                      SortOrderDTO sortOrderDto) {
    return switch (orderBy) {
      case NAME -> {
        Comparator<GroupListing> comparator = Comparator.comparing(GroupListing::groupId);
        yield loadDescriptionsByListings(ac, groups, comparator, pageNum, perPage, sortOrderDto);
      }
      case STATE -> {
        ToIntFunction<GroupListing> statesPriorities =
            cg -> switch (cg.groupState().orElse(GroupState.UNKNOWN)) {
                  case STABLE -> 0;
                  case COMPLETING_REBALANCE -> 1;
                  case PREPARING_REBALANCE -> 2;
                  case EMPTY -> 3;
                  case DEAD -> 4;
                  case UNKNOWN -> 5;
                  case ASSIGNING -> 6;
                  case RECONCILING -> 7;
                  default -> 5;
                };
        var comparator = Comparator.comparingInt(statesPriorities);
        yield loadDescriptionsByListings(ac, groups, comparator, pageNum, perPage, sortOrderDto);
      }
      case MEMBERS -> {
        var comparator = Comparator.<ShareGroupDescription>comparingInt(cg -> cg.members().size());
        var groupNames = groups.stream().map(GroupListing::groupId).toList();
        yield ac.describeShareGroups(groupNames)
            .map(descriptions ->
                sortAndPaginate(descriptions.values(), comparator, pageNum, perPage, sortOrderDto).toList());
      }
      case MESSAGES_BEHIND -> {

        Comparator<GroupWithDescr> comparator = Comparator.comparingLong(gwd ->
            gwd.icg.getConsumerLag() == null ? 0L : gwd.icg.getConsumerLag());

        yield loadDescriptionsByInternalShareGroups(cluster, ac, groups, comparator, pageNum, perPage, sortOrderDto);
      }

      case TOPIC_NUM -> {

        Comparator<GroupWithDescr> comparator = Comparator.comparingInt(gwd -> gwd.icg.getTopicNum());

        yield loadDescriptionsByInternalShareGroups(cluster, ac, groups, comparator, pageNum, perPage, sortOrderDto);

      }
    };
  }

  private Mono<List<ShareGroupDescription>> loadDescriptionsByListings(ReactiveAdminClient ac,
                                                                          List<GroupListing> listings,
                                                                          Comparator<GroupListing> comparator,
                                                                          OptionalInt pageNum,
                                                                          OptionalInt perPage,
                                                                          SortOrderDTO sortOrderDto) {
    List<String> sortedGroups = sortAndPaginate(listings, comparator, pageNum, perPage, sortOrderDto)
        .map(GroupListing::groupId)
        .toList();
    return ac.describeShareGroups(sortedGroups)
        .map(descrMap -> sortedGroups.stream().map(descrMap::get).toList());
  }

  private Mono<List<ShareGroupDescription>> loadDescriptionsByInternalShareGroups(
      KafkaCluster cluster,
      ReactiveAdminClient ac,
      List<GroupListing> groups,
      Comparator<GroupWithDescr> comparator,
      OptionalInt pageNum,
      OptionalInt perPage,
      SortOrderDTO sortOrderDto) {
    var groupNames = groups.stream().map(GroupListing::groupId).toList();

    return ac.describeShareGroups(groupNames)
        .flatMap(descriptionsMap -> {
              List<ShareGroupDescription> descriptions = descriptionsMap.values().stream().toList();
              return getShareGroups(cluster, ac, descriptions)
                  .map(icg -> Streams.zip(icg.stream(), descriptions.stream(), GroupWithDescr::new).toList())
                  .map(gwd -> sortAndPaginate(gwd, comparator, pageNum, perPage, sortOrderDto)
                      .map(GroupWithDescr::cgd).toList());
            }
        );

  }

  private <T> Stream<T> sortAndPaginate(Collection<T> collection,
                                        Comparator<T> comparator,
                                        OptionalInt pageNum,
                                        OptionalInt perPage,
                                        SortOrderDTO sortOrderDto) {
    Stream<T> sorted = collection.stream()
        .sorted(sortOrderDto == SortOrderDTO.ASC ? comparator : comparator.reversed());

    if (pageNum.isPresent() && perPage.isPresent()) {
      return sorted
          .skip((long) (pageNum.getAsInt() - 1) * perPage.getAsInt())
          .limit(perPage.getAsInt());
    } else {
      return sorted;
    }
  }


  public record ShareGroupsPage(List<InternalShareGroup> consumerGroups, int totalPages) {
    public static ShareGroupService.ShareGroupsPage from(List<InternalShareGroup> groups,
                                                               int totalSize,
                                                               OptionalInt pageNum,
                                                               OptionalInt perPage) {
      return new ShareGroupsPage(groups,
          (totalSize / perPage.orElse(totalSize)) + (totalSize % perPage.orElse(totalSize) == 0 ? 0 : 1)
      );
    }
  }

  private record GroupWithDescr(InternalShareGroup icg, ShareGroupDescription cgd) {
  }
}
