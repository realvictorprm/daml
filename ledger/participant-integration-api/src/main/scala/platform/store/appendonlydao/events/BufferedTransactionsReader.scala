// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.appendonlydao.events

import java.time.Instant

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.codahale.metrics.{Counter, Timer}
import com.daml.ledger.api.v1.active_contracts_service.GetActiveContractsResponse
import com.daml.ledger.api.v1.transaction.{
  TransactionTree,
  TreeEvent,
  Transaction => FlatTransaction,
}
import com.daml.ledger.api.v1.transaction_service.{
  GetFlatTransactionResponse,
  GetTransactionResponse,
  GetTransactionTreesResponse,
  GetTransactionsResponse,
}
import com.daml.ledger.participant.state.v1.{Offset, TransactionId}
import com.daml.lf.data.Ref
import com.daml.logging.LoggingContext
import com.daml.metrics.{InstrumentedSource, Metrics, Timed}
import com.daml.platform.ApiOffset
import com.daml.platform.api.v1.event.EventOps.TreeEventOps
import com.daml.platform.participant.util.LfEngineToApi
import com.daml.platform.store.appendonlydao.events.BufferedTransactionsReader.getTransactions
import com.daml.platform.store.cache.MutableCacheBackedContractStore.EventSequentialId
import com.daml.platform.store.cache.{BufferSlice, EventsBuffer}
import com.daml.platform.store.dao.LedgerDaoTransactionsReader
import com.daml.platform.store.dao.events.ContractStateEvent
import com.daml.platform.store.interfaces.TransactionLogUpdate
import com.daml.platform.store.interfaces.TransactionLogUpdate.{Transaction => TxUpdate}
import com.google.protobuf.timestamp.Timestamp

import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, ExecutionContext, Future}

private[events] class BufferedTransactionsReader(
    protected val delegate: LedgerDaoTransactionsReader,
    val transactionsBuffer: EventsBuffer[Offset, TransactionLogUpdate],
    toFlatTransaction: (TxUpdate, FilterRelation, Boolean) => Option[FlatTransaction],
    toTransactionTree: (TxUpdate, Set[Party], Boolean) => Option[TransactionTree],
    metrics: Metrics,
) extends LedgerDaoTransactionsReader {

  private val outputStreamBufferSize = 128

  override def getFlatTransactions(
      startExclusive: Offset,
      endInclusive: Offset,
      filter: FilterRelation,
      verbose: Boolean,
  )(implicit loggingContext: LoggingContext): Source[(Offset, GetTransactionsResponse), NotUsed] =
    getTransactions(transactionsBuffer)(startExclusive, endInclusive, filter, verbose)(
      toApiTx = toFlatTransaction,
      apiResponseCtor = GetTransactionsResponse(_),
      fetchTransactions = delegate.getFlatTransactions(_, _, _, _)(loggingContext),
      sourceTimer = metrics.daml.index.getFlatTransactionsSource,
      resolvedFromBufferCounter = metrics.daml.index.flatTransactionEventsResolvedFromBuffer,
      totalRetrievedCounter = metrics.daml.index.totalFlatTransactionsRetrieved,
      bufferSizeCounter =
        metrics.daml.index.flatTransactionsBufferSize, // TODO buffer here is ambiguous
      outputStreamBufferSize = outputStreamBufferSize,
    )

  override def getTransactionTrees(
      startExclusive: Offset,
      endInclusive: Offset,
      requestingParties: Set[Party],
      verbose: Boolean,
  )(implicit
      loggingContext: LoggingContext
  ): Source[(Offset, GetTransactionTreesResponse), NotUsed] =
    getTransactions(transactionsBuffer)(startExclusive, endInclusive, requestingParties, verbose)(
      toApiTx = toTransactionTree,
      apiResponseCtor = GetTransactionTreesResponse(_),
      fetchTransactions = delegate.getTransactionTrees(_, _, _, _)(loggingContext),
      sourceTimer = metrics.daml.index.getTransactionTreesSource,
      resolvedFromBufferCounter = metrics.daml.index.transactionTreeEventsResolvedFromBuffer,
      totalRetrievedCounter = metrics.daml.index.totalTransactionTreesRetrieved,
      bufferSizeCounter =
        metrics.daml.index.transactionTreesBufferSize, // TODO buffer here is ambiguous
      outputStreamBufferSize = outputStreamBufferSize,
    )

  override def lookupFlatTransactionById(
      transactionId: TransactionId,
      requestingParties: Set[Party],
  )(implicit loggingContext: LoggingContext): Future[Option[GetFlatTransactionResponse]] =
    delegate.lookupFlatTransactionById(transactionId, requestingParties)

  override def lookupTransactionTreeById(
      transactionId: TransactionId,
      requestingParties: Set[Party],
  )(implicit loggingContext: LoggingContext): Future[Option[GetTransactionResponse]] =
    delegate.lookupTransactionTreeById(transactionId, requestingParties)

  override def getActiveContracts(activeAt: Offset, filter: FilterRelation, verbose: Boolean)(
      implicit loggingContext: LoggingContext
  ): Source[GetActiveContractsResponse, NotUsed] =
    delegate.getActiveContracts(activeAt, filter, verbose)

  override def getContractStateEvents(startExclusive: (Offset, Long), endInclusive: (Offset, Long))(
      implicit loggingContext: LoggingContext
  ): Source[((Offset, Long), ContractStateEvent), NotUsed] =
    throw new UnsupportedOperationException(
      s"getContractStateEvents is not supported on ${getClass.getSimpleName}"
    )

  override def getTransactionLogUpdates(
      startExclusive: (Offset, EventSequentialId),
      endInclusive: (Offset, EventSequentialId),
  )(implicit
      loggingContext: LoggingContext
  ): Source[((Offset, EventSequentialId), TransactionLogUpdate), NotUsed] =
    throw new UnsupportedOperationException(
      s"getTransactionUpdates is not supported on ${getClass.getSimpleName}"
    )
}

private[platform] object BufferedTransactionsReader {
  type FetchTransactions[FILTER, API_RESPONSE] =
    (Offset, Offset, FILTER, Boolean) => Source[(Offset, API_RESPONSE), NotUsed]
  def apply(
      delegate: LedgerDaoTransactionsReader,
      transactionsBuffer: EventsBuffer[Offset, TransactionLogUpdate],
      lfValueTranslation: LfValueTranslation,
      metrics: Metrics,
  )(implicit
      loggingContext: LoggingContext,
      executionContext: ExecutionContext,
  ): BufferedTransactionsReader =
    new BufferedTransactionsReader(
      delegate = delegate,
      transactionsBuffer = transactionsBuffer,
      toFlatTransaction = ToFlatTransaction(_, _, _, lfValueTranslation),
      toTransactionTree = ToTransactionTree(_, _, _, lfValueTranslation),
      metrics = metrics,
    )

  private[events] def getTransactions[FILTER, API_TX, API_RESPONSE](
      transactionsBuffer: EventsBuffer[Offset, TransactionLogUpdate]
  )(
      startExclusive: Offset,
      endInclusive: Offset,
      filter: FILTER,
      verbose: Boolean,
  )(
      toApiTx: (TxUpdate, FILTER, Boolean) => Option[API_TX],
      apiResponseCtor: Seq[API_TX] => API_RESPONSE,
      fetchTransactions: FetchTransactions[FILTER, API_RESPONSE],
      sourceTimer: Timer,
      resolvedFromBufferCounter: Counter,
      totalRetrievedCounter: Counter,
      outputStreamBufferSize: Int,
      bufferSizeCounter: Counter,
  ): Source[(Offset, API_RESPONSE), NotUsed] = {
    def filterBuffered(
        slice: Vector[(Offset, TransactionLogUpdate)]
    ): Iterator[(Offset, API_RESPONSE)] =
      slice.iterator
        .collect { case (offset, tx: TxUpdate) =>
          offset -> toApiTx(tx, filter, verbose)
        }
        .collect { case (offset, Some(tx)) =>
          resolvedFromBufferCounter.inc()
          offset -> apiResponseCtor(Seq(tx))
        }

    val transactionsSource = Timed.source(
      sourceTimer, {
        // TODO: Remove the @unchecked once migrated to Scala 2.13.5 where this false positive exhaustivity check for Vectors is fixed
        (transactionsBuffer.slice(startExclusive, endInclusive): @unchecked) match {
          case BufferSlice.Empty =>
            fetchTransactions(startExclusive, endInclusive, filter, verbose)

          case BufferSlice.Prefix(slice) if slice.size <= 1 =>
            fetchTransactions(startExclusive, endInclusive, filter, verbose)

          // TODO: Implement and use Offset.predecessor
          case BufferSlice.Prefix((firstOffset: Offset, _) +: tl) =>
            fetchTransactions(startExclusive, firstOffset, filter, verbose)
              .concat(Source.fromIterator(() => filterBuffered(tl)))

          case BufferSlice.Inclusive(slice) =>
            Source.fromIterator(() => filterBuffered(slice))
        }
      }.map(tx => {
        totalRetrievedCounter.inc()
        tx
      }),
    )

    InstrumentedSource.bufferedSource(
      original = transactionsSource,
      counter = bufferSizeCounter,
      size = outputStreamBufferSize,
    )
  }

  private object ToFlatTransaction {
    def apply(
        tx: TxUpdate,
        filter: FilterRelation,
        verbose: Boolean,
        lfValueTranslation: LfValueTranslation,
    )(implicit
        loggingContext: LoggingContext,
        executionContext: ExecutionContext,
    ): Option[FlatTransaction] = {
      val aux = tx.events.filter(FlatTransactionPredicate(_, filter))
      val nonTransientIds = permanent(aux)
      val events = aux
        .filter(ev => nonTransientIds(ev.contractId))

      events.headOption.flatMap { first =>
        if (first.commandId.nonEmpty || events.nonEmpty) {
          val flatEvents =
            events.map(toFlatEvent(_, filter.keySet, verbose, lfValueTranslation)).collect {
              case Some(ev) =>
                ev
            }
          if (flatEvents.isEmpty)
            None
          else
            Some(
              FlatTransaction(
                transactionId = first.transactionId,
                commandId = first.commandId,
                workflowId = first.workflowId,
                effectiveAt = Some(instantToTimestamp(first.ledgerEffectiveTime)),
                events = flatEvents,
                offset = ApiOffset.toApiString(tx.offset),
                traceContext = None,
              )
            )
        } else None
      }
    }

    private val FlatTransactionPredicate =
      (event: TransactionLogUpdate.Event, filter: FilterRelation) =>
        if (filter.size == 1) {
          val (party, templateIds) = filter.iterator.next()
          if (templateIds.isEmpty)
            event.flatEventWitnesses.contains(party)
          else
            // Single-party request, restricted to a set of template identifiers
            event.flatEventWitnesses.contains(party) && templateIds.contains(event.templateId)
        } else {
          // Multi-party requests
          // If no party requests specific template identifiers
          val parties = filter.keySet
          if (filter.forall(_._2.isEmpty))
            event.flatEventWitnesses.intersect(parties.map(_.toString)).nonEmpty
          else {
            // If all parties request the same template identifier
            val templateIds = filter.valuesIterator.flatten.toSet
            if (filter.valuesIterator.forall(_ == templateIds)) {
              event.flatEventWitnesses.intersect(parties.map(_.toString)).nonEmpty &&
              templateIds.contains(event.templateId)
            } else {
              // If there are different template identifier but there are no wildcard parties
              val partiesAndTemplateIds = Relation.flatten(filter).toSet
              val wildcardParties = filter.filter(_._2.isEmpty).keySet
              if (wildcardParties.isEmpty) {
                partiesAndTemplateIds.exists { case (party, identifier) =>
                  event.flatEventWitnesses.contains(party) && identifier == event.templateId
                }
              } else {
                // If there are wildcard parties and different template identifiers
                partiesAndTemplateIds.exists { case (party, identifier) =>
                  event.flatEventWitnesses.contains(party) && identifier == event.templateId
                } || event.flatEventWitnesses.intersect(wildcardParties.map(_.toString)).nonEmpty
              }
            }
          }
        }

    private def permanent(events: Seq[TransactionLogUpdate.Event]): Set[ContractId] =
      events.foldLeft(Set.empty[ContractId]) {
        case (contractIds, event: TransactionLogUpdate.CreatedEvent) =>
          contractIds + event.contractId
        case (contractIds, event) if !contractIds.contains(event.contractId) =>
          contractIds + event.contractId
        case (contractIds, event) => contractIds - event.contractId
      }

    private def toFlatEvent(
        event: TransactionLogUpdate.Event,
        requestingParties: Set[Party],
        verbose: Boolean,
        lfValueTranslation: LfValueTranslation,
    )(implicit
        loggingContext: LoggingContext,
        executionContext: ExecutionContext,
    ): Option[com.daml.ledger.api.v1.event.Event] =
      event match {
        case createdEvent: TransactionLogUpdate.CreatedEvent =>
          Some(
            com.daml.ledger.api.v1.event.Event(
              event = com.daml.ledger.api.v1.event.Event.Event.Created(
                value = com.daml.ledger.api.v1.event.CreatedEvent(
                  eventId = createdEvent.eventId.toLedgerString,
                  contractId = createdEvent.contractId.coid,
                  templateId = Some(LfEngineToApi.toApiIdentifier(createdEvent.templateId)),
                  contractKey = createdEvent.contractKey
                    .map(
                      lfValueTranslation.toApiValue(
                        _,
                        verbose,
                        "create key",
                        value =>
                          lfValueTranslation.enricher
                            .enrichContractKey(createdEvent.templateId, value.value),
                      )
                    )
                    .map(Await.result(_, 1.second)),
                  createArguments = Some(
                    Await.result(
                      lfValueTranslation.toApiRecord(
                        createdEvent.createArgument,
                        verbose,
                        "create argument",
                        value =>
                          lfValueTranslation.enricher
                            .enrichContract(createdEvent.templateId, value.value),
                      ),
                      1.second,
                    )
                  ),
                  witnessParties = createdEvent.flatEventWitnesses
                    .intersect(requestingParties.map(_.toString))
                    .toSeq,
                  signatories = createdEvent.createSignatories.toSeq,
                  observers = createdEvent.createObservers.toSeq,
                  agreementText = createdEvent.createAgreementText.orElse(Some("")),
                )
              )
            )
          )
        case exercisedEvent: TransactionLogUpdate.ExercisedEvent if exercisedEvent.consuming =>
          Some(
            com.daml.ledger.api.v1.event.Event(
              event = com.daml.ledger.api.v1.event.Event.Event.Archived(
                value = com.daml.ledger.api.v1.event.ArchivedEvent(
                  eventId = exercisedEvent.eventId.toLedgerString,
                  contractId = exercisedEvent.contractId.coid,
                  templateId = Some(LfEngineToApi.toApiIdentifier(exercisedEvent.templateId)),
                  witnessParties = exercisedEvent.flatEventWitnesses
                    .intersect(requestingParties.map(_.toString))
                    .toSeq,
                )
              )
            )
          )
        case _ => None
      }
  }

  private object ToTransactionTree {
    def apply(
        tx: TxUpdate,
        requestingParties: Set[Party],
        verbose: Boolean,
        lfValueTranslation: LfValueTranslation,
    )(implicit
        loggingContext: LoggingContext,
        executionContext: ExecutionContext,
    ): Option[TransactionTree] = {
      val treeEvents = tx.events
        .collect {
          // TDT handle multi-party submissions
          case createdEvent: TransactionLogUpdate.CreatedEvent
              if createdEvent.treeEventWitnesses
                .intersect(requestingParties.asInstanceOf[Set[String]])
                .nonEmpty =>
            TreeEvent(
              TreeEvent.Kind.Created(
                com.daml.ledger.api.v1.event.CreatedEvent(
                  eventId = createdEvent.eventId.toLedgerString,
                  contractId = createdEvent.contractId.coid,
                  templateId = Some(LfEngineToApi.toApiIdentifier(createdEvent.templateId)),
                  contractKey = createdEvent.contractKey
                    .map(
                      lfValueTranslation.toApiValue(
                        _,
                        verbose,
                        "create key",
                        value =>
                          lfValueTranslation.enricher
                            .enrichContractKey(createdEvent.templateId, value.value),
                      )
                    )
                    .map(Await.result(_, 1.second)),
                  createArguments = Some(
                    Await.result(
                      lfValueTranslation.toApiRecord(
                        createdEvent.createArgument,
                        verbose,
                        "create argument",
                        value =>
                          lfValueTranslation.enricher
                            .enrichContract(createdEvent.templateId, value.value),
                      ),
                      1.second,
                    )
                  ),
                  witnessParties = createdEvent.treeEventWitnesses
                    .intersect(requestingParties.map(_.toString))
                    .toSeq,
                  signatories = createdEvent.createSignatories.toSeq,
                  observers = createdEvent.createObservers.toSeq,
                  agreementText = createdEvent.createAgreementText.orElse(Some("")),
                )
              )
            )
          case exercisedEvent: TransactionLogUpdate.ExercisedEvent
              if exercisedEvent.treeEventWitnesses
                .intersect(requestingParties.asInstanceOf[Set[String]])
                .nonEmpty =>
            TreeEvent(
              TreeEvent.Kind.Exercised(
                com.daml.ledger.api.v1.event.ExercisedEvent(
                  eventId = exercisedEvent.eventId.toLedgerString,
                  contractId = exercisedEvent.contractId.coid,
                  templateId = Some(LfEngineToApi.toApiIdentifier(exercisedEvent.templateId)),
                  choice = exercisedEvent.choice,
                  choiceArgument = Some(
                    Await.result(
                      lfValueTranslation.toApiValue(
                        exercisedEvent.exerciseArgument,
                        verbose,
                        "exercise argument",
                        value =>
                          lfValueTranslation.enricher
                            .enrichChoiceArgument(
                              exercisedEvent.templateId,
                              Ref.Name.assertFromString(exercisedEvent.choice),
                              value.value,
                            ),
                      ),
                      1.second,
                    )
                  ),
                  actingParties = exercisedEvent.actingParties.toSeq,
                  consuming = exercisedEvent.consuming,
                  witnessParties = exercisedEvent.treeEventWitnesses
                    .intersect(requestingParties.map(_.toString))
                    .toSeq,
                  childEventIds = exercisedEvent.children,
                  exerciseResult = exercisedEvent.exerciseResult
                    .map(
                      lfValueTranslation.toApiValue(
                        _,
                        verbose,
                        "exercise result",
                        value =>
                          lfValueTranslation.enricher.enrichChoiceResult(
                            exercisedEvent.templateId,
                            Ref.Name.assertFromString(exercisedEvent.choice),
                            value.value,
                          ),
                      )
                    )
                    .map(Await.result(_, 1.second)),
                )
              )
            )
        }

      if (treeEvents.isEmpty)
        Option.empty
      else {

        val visible = treeEvents.map(_.eventId)
        val visibleSet = visible.toSet
        val eventsById = treeEvents.iterator
          .map(e => e.eventId -> e.filterChildEventIds(visibleSet))
          .toMap

        // All event identifiers that appear as a child of another item in this response
        val children = eventsById.valuesIterator.flatMap(_.childEventIds).toSet

        // The roots for this request are all visible items
        // that are not a child of some other visible item
        val rootEventIds = visible.filterNot(children)

        Some(
          TransactionTree(
            transactionId = tx.transactionId,
            commandId = tx.commandId, // TDT use submitters predicate to set commandId
            workflowId = tx.workflowId,
            effectiveAt = Some(instantToTimestamp(tx.effectiveAt)),
            offset = ApiOffset.toApiString(tx.offset),
            eventsById = eventsById,
            rootEventIds = rootEventIds,
            traceContext = None,
          )
        )
      }
    }
  }

  private def instantToTimestamp(t: Instant): Timestamp =
    Timestamp(seconds = t.getEpochSecond, nanos = t.getNano)
}
