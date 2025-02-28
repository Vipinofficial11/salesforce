/*
 * Copyright © 2019 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package io.cdap.plugin.salesforce.plugin.source.batch;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.sforce.async.BulkConnection;
import com.sforce.ws.ConnectionException;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Metadata;
import io.cdap.cdap.api.annotation.MetadataProperty;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.batch.Input;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.lib.KeyValue;
import io.cdap.cdap.etl.api.Emitter;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.batch.BatchRuntimeContext;
import io.cdap.cdap.etl.api.batch.BatchSource;
import io.cdap.cdap.etl.api.batch.BatchSourceContext;
import io.cdap.cdap.etl.api.connector.Connector;
import io.cdap.plugin.common.Asset;
import io.cdap.plugin.common.LineageRecorder;
import io.cdap.plugin.salesforce.SObjectDescriptor;
import io.cdap.plugin.salesforce.SalesforceConnectionUtil;
import io.cdap.plugin.salesforce.SalesforceConstants;
import io.cdap.plugin.salesforce.SalesforceSchemaUtil;
import io.cdap.plugin.salesforce.authenticator.AuthenticatorCredentials;
import io.cdap.plugin.salesforce.plugin.OAuthInfo;
import io.cdap.plugin.salesforce.plugin.source.batch.util.SalesforceSourceConstants;
import io.cdap.plugin.salesforce.plugin.source.batch.util.SalesforceSplitUtil;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Plugin returns records from Salesforce using provided by user SOQL query or SObject. Reads data
 * in batches, every batch is processed as a separate split by mapreduce.
 */
@Plugin(type = BatchSource.PLUGIN_TYPE)
@Name(SalesforceBatchSource.NAME)
@Description("Read data from Salesforce.")
@Metadata(properties = {
    @MetadataProperty(key = Connector.PLUGIN_TYPE, value = SalesforceConstants.PLUGIN_NAME)})
public class SalesforceBatchSource extends
    BatchSource<Schema, Map<String, String>, StructuredRecord> {

  public static final String NAME = "Salesforce";

  private final SalesforceSourceConfig config;
  private Schema schema;
  private MapToRecordTransformer transformer;
  private Set<String> jobIds = new HashSet<>();
  private AuthenticatorCredentials authenticatorCredentials;

  public SalesforceBatchSource(SalesforceSourceConfig config) {
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    FailureCollector collector = pipelineConfigurer.getStageConfigurer().getFailureCollector();

    OAuthInfo oAuthInfo = config.containsMacro(SalesforceConstants.PROPERTY_OAUTH_INFO)
      ? null : SalesforceConnectionUtil.getOAuthInfo(config.getConnection(), collector);
    config.validate(collector, oAuthInfo);

    if (config.containsMacro(SalesforceSourceConstants.PROPERTY_SCHEMA)) {
      // schema will be available later during `prepareRun` stage
      pipelineConfigurer.getStageConfigurer().setOutputSchema(null);
      return;
    }

    if (config.containsMacro(SalesforceSourceConstants.PROPERTY_QUERY)
        || config.containsMacro(SalesforceSourceConstants.PROPERTY_SOBJECT_NAME)
        || oAuthInfo == null) {
      // this block will execute when connection got established but schema can not be fetched due to above macro fields
      // will validate schema later in `prepareRun` stage
      pipelineConfigurer.getStageConfigurer().setOutputSchema(config.getSchema());
      return;
    }

    schema = retrieveSchema(oAuthInfo);
    pipelineConfigurer.getStageConfigurer().setOutputSchema(schema);
  }

  @Override
  public void prepareRun(BatchSourceContext context) {
    FailureCollector collector = context.getFailureCollector();
    OAuthInfo oAuthInfo = SalesforceConnectionUtil.getOAuthInfo(config.getConnection(), collector);
    config.validate(collector, oAuthInfo); // validate when macros are already substituted
    collector.getOrThrowException();

    if (schema == null) {
      schema = retrieveSchema(oAuthInfo);
    }

    String query = config.getQuery(context.getLogicalStartTime(), oAuthInfo);
    String sObjectName = SObjectDescriptor.fromQuery(query).getName();
    String orgId = "unknown";
    try {
      orgId = config.getOrgId(oAuthInfo);
    } catch (ConnectionException exception) {
      String message = SalesforceConnectionUtil.getSalesforceErrorMessageFromException(exception);
      collector.addFailure(String.format("Unable to get organization Id due to error: %s", message),
          "Ensure Credentials are correct.");
    }
    Asset asset = Asset.builder(config.getReferenceNameOrNormalizedFQN(orgId, sObjectName))
        .setFqn(config.getFQN(orgId, sObjectName)).build();
    LineageRecorder lineageRecorder = new LineageRecorder(context, asset);
    lineageRecorder.createExternalDataset(schema);
    lineageRecorder.recordRead("Read", "Read from Salesforce",
        Preconditions.checkNotNull(schema.getFields()).stream()
            .map(Schema.Field::getName)
            .collect(Collectors.toList()));

    authenticatorCredentials = config.getConnection().getAuthenticatorCredentials();
    List<SalesforceSplit> querySplits =
        getSplits(config, authenticatorCredentials, context.getLogicalStartTime(), oAuthInfo);
    querySplits.stream().forEach(salesforceSplit -> jobIds.add(salesforceSplit.getJobId()));
    context.setInput(Input.of(config.getReferenceNameOrNormalizedFQN(orgId, sObjectName),
        new SalesforceInputFormatProvider(
            config, ImmutableMap.of(sObjectName, schema.toString()), querySplits, null)));
  }

  public static List<SalesforceSplit> getSplits(
      SalesforceSourceConfig config, AuthenticatorCredentials authenticatorCredentials,
      long logicStartTime, OAuthInfo oAuthInfo) {
    String query = config.getQuery(logicStartTime, oAuthInfo);
    BulkConnection bulkConnection = SalesforceSplitUtil.getBulkConnection(authenticatorCredentials);
    boolean enablePKChunk = config.getEnablePKChunk();
    if (enablePKChunk) {
      String parent = config.getParent();
      int chunkSize = config.getChunkSize();
      List<String> chunkHeaderValues = new ArrayList<>();
      chunkHeaderValues.add(
          String.format(SalesforceSourceConstants.HEADER_VALUE_PK_CHUNK, chunkSize));
      if (!Strings.isNullOrEmpty(parent)) {
        chunkHeaderValues.add(
            String.format(SalesforceSourceConstants.HEADER_PK_CHUNK_PARENT, parent));
      }
      bulkConnection.addHeader(SalesforceSourceConstants.HEADER_ENABLE_PK_CHUNK,
          String.join(";", chunkHeaderValues));
    }
    List<SalesforceSplit> querySplits = SalesforceSplitUtil.getQuerySplits(query, bulkConnection,
        enablePKChunk, config.getOperation(), config.getInitialRetryDuration(), config.getMaxRetryDuration(),
          config.getMaxRetryCount(), config.isRetryRequired());
    return querySplits;
  }

  @Override
  public void initialize(BatchRuntimeContext context) throws Exception {
    super.initialize(context);
    this.transformer = new MapToRecordTransformer();
  }

  @Override
  public void onRunFinish(boolean succeeded, BatchSourceContext context) {
    super.onRunFinish(succeeded, context);
    SalesforceSplitUtil.closeJobs(jobIds, authenticatorCredentials);
  }

  @Override
  public void transform(KeyValue<Schema, Map<String, String>> input,
      Emitter<StructuredRecord> emitter) throws Exception {
    StructuredRecord record = transformer.transform(input.getKey(), input.getValue());
    emitter.emit(record);
  }

  /**
   * Get Salesforce schema by query
   *
   * @param config Salesforce Source Batch config
   * @return schema calculated from query
   */
  public static Schema getSchema(SalesforceSourceConfig config, OAuthInfo oAuthInfo) {
    return getSchema(config, oAuthInfo, false);
  }

  /**
   * Get Salesforce schema by query, with the option to allow null values in non-nullable custom fields
   *
   * @param config Salesforce Source Batch config
   * @param setAllCustomFieldsNullable set all custom fields nullable by default
   * @return schema calculated from query
   */
  public static Schema getSchema(SalesforceSourceConfig config, OAuthInfo oAuthInfo,
                                 boolean setAllCustomFieldsNullable) {
    String query = config.getQuery(System.currentTimeMillis(), oAuthInfo);
    SObjectDescriptor sObjectDescriptor = SObjectDescriptor.fromQuery(query);
    try {
      AuthenticatorCredentials credentials = AuthenticatorCredentials.fromParameters(oAuthInfo,
                                                                          config.getConnection().getConnectTimeout(),
                                                                          config.getConnection().getReadTimeout(),
                                                                          config.getConnection().getProxyUrl());
      return SalesforceSchemaUtil.getSchema(credentials, sObjectDescriptor, setAllCustomFieldsNullable);
    } catch (ConnectionException e) {
      String errorMessage = SalesforceConnectionUtil.getSalesforceErrorMessageFromException(e);
      throw new RuntimeException(
          String.format("Failed to get schema from the query '%s': %s", query, errorMessage),
          e);
    }
  }

  /**
   * Retrieves provided and actual schemas. If both schemas are available, validates their
   * compatibility.
   *
   * @return provided schema if present, otherwise actual schema
   */
  @VisibleForTesting
  public Schema retrieveSchema(OAuthInfo oAuthInfo) {
    Schema providedSchema = config.getSchema();
    Schema actualSchema = getSchema(config, oAuthInfo);
    if (providedSchema != null) {
      SalesforceSchemaUtil.checkCompatibility(actualSchema, providedSchema);
      return providedSchema;
    }
    return actualSchema;
  }

}
