import "@typespec/openapi";
import "./responses.tsp";

namespace Api;

using TypeSpec.Http;
using OpenAPI;

@route("/api/clusters/{clusterName}/share-groups")
@tag("Share Groups")
interface ShareGroupsApi {
  @get
  @route("/paged")
  @operationId("getShareGroupsPage")
  @summary("getShareGroupsPage")
  getShareGroupsPage(
    @path clusterName: string,
    @query page?: int32,
    @query perPage?: int32,
    @query search?: string,
    @query orderBy?: ShareGroupOrdering,
    @query sortOrder?: SortOrder,
    @query fts?: boolean,
    @query state?: ShareGroupState[]
  ): ShareGroupsPageResponse;

  @get
  @route("/lag")
  @operationId("getShareGroupsLag")
  @summary("getShareGroupsLag")
  getShareGroupsLag(
    @path clusterName: string,
    @query ids: Array<string>,
    @query lastUpdate?: int64,
    @query includePartitions?: boolean
  ): ShareGroupsLagResponse;

  @get
  @route("/csv")
  @operationId("getShareGroupsCsv")
  @summary("getShareGroupsPage")
  getShareGroupsCsv(
    @path clusterName: string,
    @query page?: int32,
    @query perPage?: int32,
    @query search?: string,
    @query orderBy?: ShareGroupOrdering,
    @query sortOrder?: SortOrder,
    @query fts?: boolean,
    @query state?: ShareGroupState[]
  ): CsvResponse;

  @get
  @route("/{id}")
  @operationId("getShareGroup")
  @summary("getShareGroup")
  getShareGroup(
    @path clusterName: string,
    @path id: string,
  ): ShareGroupDetails;
}

@route("/api/clusters/{clusterName}/topics/{topicName}/share-groups")
@tag("Share Groups")
interface TopicShareGroupsApi {
  @get
  @operationId("getTopicShareGroups")
  @summary("getTopicShareGroups")
  getTopicShareGroups(
    @path clusterName: string,
    @path topicName: string,
  ): ShareGroup[];
}

enum ShareGroupState {
  UNKNOWN,
  PREPARING_REBALANCE,
  COMPLETING_REBALANCE,
  STABLE,
  DEAD,
  EMPTY,
}

@discriminator("inherit")
model ShareGroup {
  @invisible(Lifecycle)
  inherit: string;
  groupId: string;
  members?: int32;
  topics?: int32;
  simple?: boolean;
  partitionAssignor?: string;
  state?: ShareGroupState;
  coordinator?: Broker;

  @doc("null if consumer group has no offsets committed")
  consumerLag?: int64;
}


model ShareGroupDetails extends ShareGroup {
  @invisible(Lifecycle)
  inherit: "details";
  partitions?: ShareGroupTopicPartition[];
}

enum ShareGroupOrdering {
  NAME,
  MEMBERS,
  STATE,
  MESSAGES_BEHIND,
  TOPIC_NUM,
}

model ShareGroupsPageResponse {
  pageCount?: int32;
  shareGroups?: ShareGroup[];
}

model ShareGroupOffsetsReset {
  topic: string;
  resetType: ShareGroupOffsetsResetType;
  partitions?: int32[];
  resetToTimestamp?: int64;
  partitionsOffsets?: PartitionOffset[];
}

model ShareGroupsLagResponse {
    updateTimestamp: int64;
    shareGroups: Record<ShareGroupLag>;
}

model ShareGroupLag {
    lag: int64;
    topics: Record<int64>;
    topicPartitions?: Record<ShareGroupTopicLag>;
}

model ShareGroupTopicLag {
    partitions?: Record<int64>;
}

enum ShareGroupOffsetsResetType {
  EARLIEST,
  LATEST,
  TIMESTAMP,
  OFFSET,
}

model ShareGroupTopicPartition {
  topic: string;
  partition: int32;
  currentOffset?: int64;
  endOffset?: int64;
  consumerLag?: int64; // null if consumer group has no offsets committed
  consumerId?: string;
  host?: string;
}


