// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.participant.state.kvutils.app

import akka.stream.Materializer
import com.codahale.metrics.SharedMetricRegistries
import com.daml.ledger.api.auth.{AuthService, AuthServiceWildcard}
import com.daml.ledger.participant.state.kvutils.api.{KeyValueLedger, KeyValueParticipantState}
import com.daml.ledger.participant.state.v1.{ReadService, WriteService}
import com.daml.ledger.resources.ResourceOwner
import com.daml.lf.engine.Engine
import com.daml.logging.LoggingContext
import com.daml.metrics.Metrics
import com.daml.platform.apiserver.{ApiServerConfig, TimeServiceBackend}
import com.daml.platform.configuration.{LedgerConfiguration, PartyConfiguration}
import com.daml.platform.indexer.{IndexerConfig, IndexerStartupMode}
import io.grpc.ServerInterceptor
import scopt.OptionParser

import java.util.concurrent.TimeUnit
import scala.annotation.nowarn
import scala.concurrent.duration.FiniteDuration

@nowarn("msg=parameter value config .* is never used") // possibly used in overrides
trait ConfigProvider[ExtraConfig] {
  val defaultExtraConfig: ExtraConfig

  def extraConfigParser(parser: OptionParser[Config[ExtraConfig]]): Unit

  def manipulateConfig(config: Config[ExtraConfig]): Config[ExtraConfig] =
    config

  def indexerConfig(
      participantConfig: ParticipantConfig,
      config: Config[ExtraConfig],
  ): IndexerConfig =
    IndexerConfig(
      participantConfig.participantId,
      jdbcUrl = participantConfig.serverJdbcUrl,
      databaseConnectionPoolSize = participantConfig.indexerConfig.databaseConnectionPoolSize,
      startupMode = IndexerStartupMode.MigrateAndStart,
      eventsPageSize = config.eventsPageSize,
      allowExistingSchema = participantConfig.indexerConfig.allowExistingSchema,
      enableAppendOnlySchema = config.enableAppendOnlySchema,
      maxInputBufferSize = participantConfig.indexerConfig.maxInputBufferSize,
      inputMappingParallelism = participantConfig.indexerConfig.ingestionParallelism,
      batchingParallelism = participantConfig.indexerConfig.batchingParallelism,
      submissionBatchSize = participantConfig.indexerConfig.submissionBatchSize,
      tailingRateLimitPerSecond = participantConfig.indexerConfig.tailingRateLimitPerSecond,
      batchWithinMillis = participantConfig.indexerConfig.batchWithinMillis,
      enableCompression = participantConfig.indexerConfig.enableCompression,
    )

  def apiServerConfig(
      participantConfig: ParticipantConfig,
      config: Config[ExtraConfig],
  ): ApiServerConfig =
    ApiServerConfig(
      participantId = participantConfig.participantId,
      archiveFiles = config.archiveFiles.map(_.toFile).toList,
      port = participantConfig.port,
      address = participantConfig.address,
      jdbcUrl = participantConfig.serverJdbcUrl,
      databaseConnectionPoolSize = participantConfig.apiServerDatabaseConnectionPoolSize,
      databaseConnectionTimeout = FiniteDuration(
        participantConfig.apiServerDatabaseConnectionTimeout.toMillis,
        TimeUnit.MILLISECONDS,
      ),
      tlsConfig = config.tlsConfig,
      maxInboundMessageSize = config.maxInboundMessageSize,
      eventsPageSize = config.eventsPageSize,
      portFile = participantConfig.portFile,
      seeding = config.seeding,
      managementServiceTimeout = participantConfig.managementServiceTimeout,
      enableAppendOnlySchema = config.enableAppendOnlySchema,
      maxContractStateCacheSize = participantConfig.maxContractStateCacheSize,
      maxContractKeyStateCacheSize = participantConfig.maxContractKeyStateCacheSize,
      enableMutableContractStateCache = config.enableMutableContractStateCache,
      maxTransactionsInMemoryFanOutBufferSize =
        participantConfig.maxTransactionsInMemoryFanOutBufferSize,
      enableInMemoryFanOutForLedgerApi = config.enableInMemoryFanOutForLedgerApi,
    )

  def partyConfig(config: Config[ExtraConfig]): PartyConfiguration =
    PartyConfiguration.default

  def ledgerConfig(config: Config[ExtraConfig]): LedgerConfiguration =
    LedgerConfiguration.defaultRemote

  def timeServiceBackend(config: Config[ExtraConfig]): Option[TimeServiceBackend] = None

  def authService(config: Config[ExtraConfig]): AuthService =
    AuthServiceWildcard

  def interceptors(config: Config[ExtraConfig]): List[ServerInterceptor] =
    List.empty

  def createMetrics(
      participantConfig: ParticipantConfig,
      config: Config[ExtraConfig],
  ): Metrics = {
    val registryName = participantConfig.participantId + participantConfig.shardName
      .map("-" + _)
      .getOrElse("")
    new Metrics(SharedMetricRegistries.getOrCreate(registryName))
  }
}

trait ReadServiceOwner[+RS <: ReadService, ExtraConfig] extends ConfigProvider[ExtraConfig] {
  def readServiceOwner(
      config: Config[ExtraConfig],
      participantConfig: ParticipantConfig,
      engine: Engine,
  )(implicit materializer: Materializer, loggingContext: LoggingContext): ResourceOwner[RS]
}

trait WriteServiceOwner[+WS <: WriteService, ExtraConfig] extends ConfigProvider[ExtraConfig] {
  def writeServiceOwner(
      config: Config[ExtraConfig],
      participantConfig: ParticipantConfig,
      engine: Engine,
  )(implicit materializer: Materializer, loggingContext: LoggingContext): ResourceOwner[WS]
}

trait LedgerFactory[+RWS <: ReadWriteService, ExtraConfig]
    extends ReadServiceOwner[RWS, ExtraConfig]
    with WriteServiceOwner[RWS, ExtraConfig] {

  override final def readServiceOwner(
      config: Config[ExtraConfig],
      participantConfig: ParticipantConfig,
      engine: Engine,
  )(implicit materializer: Materializer, loggingContext: LoggingContext): ResourceOwner[RWS] =
    readWriteServiceOwner(config, participantConfig, engine)

  override final def writeServiceOwner(
      config: Config[ExtraConfig],
      participantConfig: ParticipantConfig,
      engine: Engine,
  )(implicit materializer: Materializer, loggingContext: LoggingContext): ResourceOwner[RWS] =
    readWriteServiceOwner(config, participantConfig, engine)

  def readWriteServiceOwner(
      config: Config[ExtraConfig],
      participantConfig: ParticipantConfig,
      engine: Engine,
  )(implicit materializer: Materializer, loggingContext: LoggingContext): ResourceOwner[RWS]
}

object LedgerFactory {

  abstract class KeyValueLedgerFactory[KVL <: KeyValueLedger]
      extends LedgerFactory[KeyValueParticipantState, Unit] {
    override final val defaultExtraConfig: Unit = ()

    override final def readWriteServiceOwner(
        config: Config[Unit],
        participantConfig: ParticipantConfig,
        engine: Engine,
    )(implicit
        materializer: Materializer,
        loggingContext: LoggingContext,
    ): ResourceOwner[KeyValueParticipantState] =
      for {
        readerWriter <- owner(config, participantConfig, engine)
      } yield new KeyValueParticipantState(
        readerWriter,
        readerWriter,
        createMetrics(participantConfig, config),
      )

    def owner(
        value: Config[Unit],
        config: ParticipantConfig,
        engine: Engine,
    )(implicit materializer: Materializer, loggingContext: LoggingContext): ResourceOwner[KVL]

    override final def extraConfigParser(parser: OptionParser[Config[Unit]]): Unit =
      ()
  }
}
