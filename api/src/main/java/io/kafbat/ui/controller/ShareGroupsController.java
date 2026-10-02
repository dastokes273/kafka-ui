package io.kafbat.ui.controller;

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
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import java.util.List;

import static io.kafbat.ui.model.rbac.permission.ConsumerGroupAction.VIEW;

@RestController
@RequiredArgsConstructor
@Slf4j
public class ShareGroupsController extends AbstractController implements ShareGroupsApi, McpTool  {

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
        .then(shareGroupService.getConsumerShareGroupDetail(getCluster(clusterName), consumerShareGroupId)
            .map(ShareGroupMapper::toDetailsDto)
            .map(ResponseEntity::ok))
        .doOnEach(sig -> audit(context, sig));

  }

  @Override
  public Mono<ResponseEntity<String>> getShareGroupsCsv(String clusterName, Integer page, Integer perPage,
                                                                String search, ShareGroupOrderingDTO orderBy,
                                                                SortOrderDTO sortOrder, Boolean fts,
                                                                List<ShareGroupStateDTO> state,
                                                                ServerWebExchange exchange) {
    return null;
  }

  @Override
  public Mono<ResponseEntity<ShareGroupsLagResponseDTO>> getShareGroupsLag(String clusterName,
                                                                                           List<String> ids,
                                                                                           Long lastUpdate,
                                                                                           Boolean includePartitions,
                                                                                           ServerWebExchange exchange) {
    return null;
  }

  @Override
  public Mono<ResponseEntity<ShareGroupsPageResponseDTO>> getShareGroupsPage(String clusterName,
                                                                                             Integer page,
                                                                                             Integer perPage,
                                                                                             String search,
                                                                                             ShareGroupOrderingDTO orderBy,
                                                                                             SortOrderDTO sortOrder,
                                                                                             Boolean fts,
                                                                                             List<ShareGroupStateDTO> state,
                                                                                             ServerWebExchange exchange) {
    return null;
  }

  @Override
  public Mono<ResponseEntity<Flux<ShareGroupDTO>>> getTopicShareGroups(String clusterName,
                                                                                       String topicName,
                                                                                       ServerWebExchange exchange) {
    return null;
  }
}
