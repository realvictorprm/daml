// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.validator

import com.daml.ledger.participant.state.kvutils.DamlKvutils.{DamlStateKey, DamlStateValue}
import com.daml.ledger.participant.state.kvutils.Envelope
import com.daml.ledger.validator.LedgerStateOperations.{Key, Value}
import com.daml.ledger.validator.LogAppendingCommitStrategySpec._
import com.daml.ledger.validator.TestHelper._
import com.google.protobuf.ByteString
import org.mockito.ArgumentMatchers
import org.mockito.ArgumentMatchers._
import org.mockito.Mockito.{times, verify, when}
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{AsyncWordSpec, Matchers}

import scala.concurrent.{ExecutionContext, Future}

final class LogAppendingCommitStrategySpec extends AsyncWordSpec with Matchers with MockitoSugar {
  "commit" should {
    "return index from appendToLog" in {
      val mockLedgerStateOperations = mock[LedgerStateOperations[Long]]
      val expectedIndex = 1234L
      when(mockLedgerStateOperations.appendToLog(any[Key](), any[Value]())(any[ExecutionContext]()))
        .thenReturn(Future.successful(expectedIndex))
      val instance =
        new LogAppendingCommitStrategy[Long](
          mockLedgerStateOperations,
          DefaultStateKeySerializationStrategy)

      instance
        .commit(aParticipantId, "a correlation ID", aLogEntryId(), aLogEntry, Map.empty, Map.empty)
        .map { actualIndex =>
          verify(mockLedgerStateOperations, times(1)).appendToLog(any[Key](), any[Value]())(
            any[ExecutionContext]())
          verify(mockLedgerStateOperations, times(0))
            .writeState(any[Seq[(Key, Value)]]())(any[ExecutionContext]())
          actualIndex should be(expectedIndex)
        }
    }

    "write keys serialized according to strategy" in {
      val mockLedgerStateOperations = mock[LedgerStateOperations[Long]]
      when(
        mockLedgerStateOperations.writeState(any[Iterable[(Key, Value)]])(any[ExecutionContext]()))
        .thenReturn(Future.unit)
      when(mockLedgerStateOperations.appendToLog(any[Key], any[Value])(any[ExecutionContext]()))
        .thenReturn(Future.successful(0L))
      val mockStateKeySerializationStrategy = mock[StateKeySerializationStrategy]
      val expectedStateKey = ByteString.copyFromUtf8("some key")
      when(mockStateKeySerializationStrategy.serializeStateKey(aStateKey))
        .thenReturn(expectedStateKey)
      val expectedOutputStateBytes = Map(expectedStateKey -> Envelope.enclose(aStateValue))
      val instance =
        new LogAppendingCommitStrategy[Long](
          mockLedgerStateOperations,
          mockStateKeySerializationStrategy)

      instance
        .commit(
          aParticipantId,
          "a correlation ID",
          aLogEntryId(),
          aLogEntry,
          Map.empty,
          Map(aStateKey -> aStateValue))
        .map { _: Long =>
          verify(mockStateKeySerializationStrategy, times(1)).serializeStateKey(aStateKey)
          verify(mockLedgerStateOperations, times(1))
            .writeState(ArgumentMatchers.eq(expectedOutputStateBytes))(any[ExecutionContext]())
          succeed
        }
    }
  }
}

object LogAppendingCommitStrategySpec {
  private val aStateKey: DamlStateKey = DamlStateKey
    .newBuilder()
    .setContractId(1.toString)
    .build

  private val aStateValue: DamlStateValue = DamlStateValue.getDefaultInstance
}
