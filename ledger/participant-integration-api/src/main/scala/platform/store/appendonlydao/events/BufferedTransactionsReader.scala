// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.appendonlydao.events

import java.time.Instant

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.codahale.metrics.{Counter, Timer}
import com.daml.ledger.api.v1
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
import com.daml.platform.store.interfaces.TransactionLogUpdate.{
  CreatedEvent,
  ExercisedEvent,
  Transaction => TxUpdate,
}
import com.google.protobuf.timestamp.Timestamp

import scala.concurrent.{ExecutionContext, Future}

private[events] class BufferedTransactionsReader(
    protected val delegate: LedgerDaoTransactionsReader,
    val transactionsBuffer: EventsBuffer[Offset, TransactionLogUpdate],
    toFlatTransaction: (TxUpdate, FilterRelation, Boolean) => Future[Option[FlatTransaction]],
    toTransactionTree: (TxUpdate, Set[Party], Boolean) => Future[Option[TransactionTree]],
    metrics: Metrics,
)(implicit executionContext: ExecutionContext)
    extends LedgerDaoTransactionsReader {

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
      toApiTx: (TxUpdate, FILTER, Boolean) => Future[Option[API_TX]],
      apiResponseCtor: Seq[API_TX] => API_RESPONSE,
      fetchTransactions: FetchTransactions[FILTER, API_RESPONSE],
      sourceTimer: Timer,
      resolvedFromBufferCounter: Counter,
      totalRetrievedCounter: Counter,
      outputStreamBufferSize: Int,
      bufferSizeCounter: Counter,
  )(implicit executionContext: ExecutionContext): Source[(Offset, API_RESPONSE), NotUsed] = {
    def filterBuffered(
        slice: Vector[(Offset, TransactionLogUpdate)]
    ): Future[Iterator[(Offset, API_RESPONSE)]] =
      Future
        .traverse(
          slice.iterator
            .collect { case (offset, tx: TxUpdate) =>
              toApiTx(tx, filter, verbose).map(offset -> _)
            }
        )(identity)
        .map(_.collect { case (offset, Some(tx)) =>
          resolvedFromBufferCounter.inc()
          offset -> apiResponseCtor(Seq(tx))
        })

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
              .concat(
                Source.futureSource(filterBuffered(tl).map(it => Source.fromIterator(() => it)))
              )
              .mapMaterializedValue(_ => NotUsed)

          case BufferSlice.Inclusive(slice) =>
            Source
              .futureSource(filterBuffered(slice).map(it => Source.fromIterator(() => it)))
              .mapMaterializedValue(_ => NotUsed)
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
    ): Future[Option[FlatTransaction]] = {
      val aux = tx.events.filter(FlatTransactionPredicate(_, filter))
      val nonTransientIds = permanent(aux)
      val events = aux.filter(ev => nonTransientIds(ev.contractId))

      events.headOption
        .collect {
          case first if first.commandId.nonEmpty || events.nonEmpty =>
            Future
              .traverse(events)(toFlatEvent(_, filter.keySet, verbose, lfValueTranslation))
              .map(_.flatten)
              .map {
                case Vector() => None
                case flatEvents =>
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
              }
        }
        .getOrElse(Future.successful(None))
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
    ): Future[Option[com.daml.ledger.api.v1.event.Event]] =
      event match {
        case createdEvent: TransactionLogUpdate.CreatedEvent =>
          createdToFlatEvent(requestingParties, verbose, lfValueTranslation, createdEvent)

        case exercisedEvent: TransactionLogUpdate.ExercisedEvent if exercisedEvent.consuming =>
          Future.successful(Some(exercisedToFlatEvent(requestingParties, exercisedEvent)))

        case _ => Future.successful(None)
      }

    private def createdToFlatEvent(
        requestingParties: Set[Party],
        verbose: Boolean,
        lfValueTranslation: LfValueTranslation,
        createdEvent: CreatedEvent,
    )(implicit
        loggingContext: LoggingContext,
        executionContext: ExecutionContext,
    ) = {
      val eventualContractKey = createdEvent.contractKey
        .map(
          lfValueTranslation
            .toApiValue(
              _,
              verbose,
              "create key",
              value =>
                lfValueTranslation.enricher
                  .enrichContractKey(createdEvent.templateId, value.value),
            )
            .map(Some(_))
        )
        .getOrElse(Future.successful(None))

      val eventualCreateArguments = lfValueTranslation.toApiRecord(
        createdEvent.createArgument,
        verbose,
        "create argument",
        value =>
          lfValueTranslation.enricher
            .enrichContract(createdEvent.templateId, value.value),
      )

      for {
        maybeContractKey <- eventualContractKey
        createArguments <- eventualCreateArguments
      } yield Some(
        com.daml.ledger.api.v1.event.Event(
          com.daml.ledger.api.v1.event.Event.Event.Created(
            com.daml.ledger.api.v1.event.CreatedEvent(
              eventId = createdEvent.eventId.toLedgerString,
              contractId = createdEvent.contractId.coid,
              templateId = Some(LfEngineToApi.toApiIdentifier(createdEvent.templateId)),
              contractKey = maybeContractKey,
              createArguments = Some(createArguments),
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
    }
  }

  private def exercisedToFlatEvent(
      requestingParties: Set[Party],
      exercisedEvent: ExercisedEvent,
  ) =
    v1.event.Event(
      v1.event.Event.Event.Archived(
        v1.event.ArchivedEvent(
          eventId = exercisedEvent.eventId.toLedgerString,
          contractId = exercisedEvent.contractId.coid,
          templateId = Some(LfEngineToApi.toApiIdentifier(exercisedEvent.templateId)),
          witnessParties = exercisedEvent.flatEventWitnesses
            .intersect(requestingParties.map(_.toString))
            .toSeq,
        )
      )
    )

  private object ToTransactionTree {
    def apply(
        tx: TxUpdate,
        requestingParties: Set[Party],
        verbose: Boolean,
        lfValueTranslation: LfValueTranslation,
    )(implicit
        loggingContext: LoggingContext,
        executionContext: ExecutionContext,
    ): Future[Option[TransactionTree]] = {
      val treeEvents = tx.events
        .filter(TransactionTreePredicate(requestingParties))
        .collect {
          // TDT handle multi-party submissions
          case createdEvent: TransactionLogUpdate.CreatedEvent =>
            createdToTransactionTreeEvent(
              requestingParties,
              verbose,
              lfValueTranslation,
              createdEvent,
            )
          case exercisedEvent: TransactionLogUpdate.ExercisedEvent =>
            exercisedToTransactionTreeEvent(
              requestingParties,
              verbose,
              lfValueTranslation,
              exercisedEvent,
            )
        }

      if (treeEvents.isEmpty)
        Future.successful(Option.empty)
      else
        Future.traverse(treeEvents)(identity).map { treeEvents =>
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

    private def exercisedToTransactionTreeEvent(
        requestingParties: Set[Party],
        verbose: Boolean,
        lfValueTranslation: LfValueTranslation,
        exercisedEvent: ExercisedEvent,
    )(implicit
        loggingContext: LoggingContext,
        executionContext: ExecutionContext,
    ) = {
      val eventualChoiceArgument = lfValueTranslation.toApiValue(
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
      )

      val eventualExerciseResult = exercisedEvent.exerciseResult
        .map(
          lfValueTranslation
            .toApiValue(
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
            .map(Some(_))
        )
        .getOrElse(Future.successful(None))

      for {
        choiceArgument <- eventualChoiceArgument
        maybeExerciseResult <- eventualExerciseResult
      } yield TreeEvent(
        TreeEvent.Kind.Exercised(
          v1.event.ExercisedEvent(
            eventId = exercisedEvent.eventId.toLedgerString,
            contractId = exercisedEvent.contractId.coid,
            templateId = Some(LfEngineToApi.toApiIdentifier(exercisedEvent.templateId)),
            choice = exercisedEvent.choice,
            choiceArgument = Some(choiceArgument),
            actingParties = exercisedEvent.actingParties.toSeq,
            consuming = exercisedEvent.consuming,
            witnessParties = exercisedEvent.treeEventWitnesses
              .intersect(requestingParties.map(_.toString))
              .toSeq,
            childEventIds = exercisedEvent.children,
            exerciseResult = maybeExerciseResult,
          )
        )
      )
    }

    private def createdToTransactionTreeEvent(
        requestingParties: Set[Party],
        verbose: Boolean,
        lfValueTranslation: LfValueTranslation,
        createdEvent: CreatedEvent,
    )(implicit
        loggingContext: LoggingContext,
        executionContext: ExecutionContext,
    ) = {
      val eventualContractKey = createdEvent.contractKey
        .map(
          lfValueTranslation
            .toApiValue(
              _,
              verbose,
              "create key",
              value =>
                lfValueTranslation.enricher
                  .enrichContractKey(createdEvent.templateId, value.value),
            )
            .map(Some(_))
        )
        .getOrElse(Future.successful(None))

      val eventualCreateArguments = lfValueTranslation.toApiRecord(
        createdEvent.createArgument,
        verbose,
        "create argument",
        value =>
          lfValueTranslation.enricher
            .enrichContract(createdEvent.templateId, value.value),
      )

      for {
        maybeContractKey <- eventualContractKey
        createArguments <- eventualCreateArguments
      } yield TreeEvent(
        TreeEvent.Kind.Created(
          com.daml.ledger.api.v1.event.CreatedEvent(
            eventId = createdEvent.eventId.toLedgerString,
            contractId = createdEvent.contractId.coid,
            templateId = Some(LfEngineToApi.toApiIdentifier(createdEvent.templateId)),
            contractKey = maybeContractKey,
            createArguments = Some(createArguments),
            witnessParties = createdEvent.treeEventWitnesses
              .intersect(requestingParties.map(_.toString))
              .toSeq,
            signatories = createdEvent.createSignatories.toSeq,
            observers = createdEvent.createObservers.toSeq,
            agreementText = createdEvent.createAgreementText.orElse(Some("")),
          )
        )
      )
    }

    private val TransactionTreePredicate: Set[Party] => TransactionLogUpdate.Event => Boolean =
      requestingParties => {
        case createdEvent: CreatedEvent =>
          createdEvent.treeEventWitnesses
            .intersect(requestingParties.map(_.toString))
            .nonEmpty
        case exercised: ExercisedEvent =>
          exercised.treeEventWitnesses
            .intersect(requestingParties.map(_.toString))
            .nonEmpty
      }
  }

  private def instantToTimestamp(t: Instant): Timestamp =
    Timestamp(seconds = t.getEpochSecond, nanos = t.getNano)
}
