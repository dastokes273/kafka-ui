package io.kafbat.ui.controller;

import static io.kafbat.ui.model.rbac.permission.ConsumerGroupAction.VIEW;

import io.kafbat.ui.api.ShareGroupsApi;
import io.kafbat.ui.mapper.ShareGroupMapper;
import io.kafbat.ui.model.ShareGroupDTO;
import io.kafbat.ui.model.ShareGroupDetailsDTO;
import io.kafbat.ui.model.ShareGroupOrderingDTO;
import io.kafbat.ui.model.ShareGroupStateDTO;
import io.kafbat.ui.model.ShareGroupsLagResponseDTO;
import io.kafbat.ui.model.ShareGroupsPageResponseDTO;
import io.kafbat.ui.model.SortOrderDTO;
import io.kafbat.ui.model.rbac.AccessContext;
import io.kafbat.ui.service.ShareGroupService;
import io.kafbat.ui.service.mcp.McpTool;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@RestController
@RequiredArgsConstructor
@Slf4j
public class ShareGroupsController extends AbstractController implements ShareGroupsApi, McpTool {

  private final ShareGroupService shareGroupService;

  @Value("${consumer.groups.page.size:25}")
  private int defaultConsumerGroupsPageSize;

  @Override
  public Mono<ResponseEntity<ShareGroupDetailsDTO>> getShareGroup(String clusterName, String consumerShareGroupId,
                                                                  ServerWebExchange exchange) {
    var context = AccessContext.builder()
        .cluster(clusterName)
        .consumerGroupActions(consumerShareGroupId, VIEW)
        .operationName("getConsumerGroup")
        .build();

    return validateAccess(context)
        .then(shareGroupService.getShareGroupDetail(getCluster(clusterName), consumerShareGroupId)
            .map(ShareGroupMapper::toDetailsDto)
            .map(ResponseEntity::ok))
        .doOnEach(sig -> audit(context, sig));

  }

  @Override
  public Mono<ResponseEntity<String>> getShareGroupsCsv(
      String clusterName, Integer page, Integer perPage,
      String search, ShareGroupOrderingDTO orderBy,
      SortOrderDTO sortOrder, Boolean fts,
      List<ShareGroupStateDTO> state,
      ServerWebExchange exchange) {
    return null;
  }

  @Override
  public Mono<ResponseEntity<ShareGroupsLagResponseDTO>> getShareGroupsLag(
      String clusterName,
      List<String> ids,
      Long lastUpdate,
      Boolean includePartitions,
      ServerWebExchange exchange) {
    return null;
  }

  @Override
  public Mono<ResponseEntity<ShareGroupsPageResponseDTO>> getShareGroupsPage(
      String clusterName,
      Integer page,
      Integer perPage,
      String search,
      ShareGroupOrderingDTO orderBy,
      SortOrderDTO sortOrder,
      Boolean fts,
      List<ShareGroupStateDTO> state,
      ServerWebExchange exchange) {

    var context = AccessContext.builder()
        .cluster(clusterName)
        // consumer group access validation is within the service
        .operationName("getConsumerGroupsPage")
        .build();

    return validateAccess(context).then(
        shareGroupService.getShareGroups(
                getCluster(clusterName),
                OptionalInt.of(
                    Optional.ofNullable(page).filter(i -> i > 0).orElse(1)
                ),
                OptionalInt.of(
                    Optional.ofNullable(perPage).filter(i -> i > 0).orElse(defaultConsumerGroupsPageSize)
                ),
                search,
                fts,
                Optional.ofNullable(orderBy).orElse(ShareGroupOrderingDTO.NAME),
                Optional.ofNullable(sortOrder).orElse(SortOrderDTO.ASC),
                Optional.ofNullable(state).orElse(List.of())
            )
            .map(this::convertPage)
            .map(ResponseEntity::ok)
    ).doOnEach(sig -> audit(context, sig));

  }

  @Override
  public Mono<ResponseEntity<Flux<ShareGroupDTO>>> getTopicShareGroups(
      String clusterName,
      String topicName,
      ServerWebExchange exchange) {
    return null;
  }

  private ShareGroupsPageResponseDTO convertPage(ShareGroupService.ShareGroupsPage
                                                        shareGroupsPage) {
    return new ShareGroupsPageResponseDTO()
        .pageCount(shareGroupsPage.totalPages())
        .shareGroups(shareGroupsPage.consumerGroups()
            .stream()
            .map(ShareGroupMapper::toDto)
            .toList());
  }
}
