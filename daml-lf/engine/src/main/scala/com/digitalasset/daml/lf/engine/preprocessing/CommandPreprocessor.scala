// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf
package engine
package preprocessing

import com.daml.lf.data._
import com.daml.lf.language.Ast
import com.daml.lf.speedy.SValue
import com.daml.lf.value.Value

import scala.annotation.tailrec

private[lf] final class CommandPreprocessor(compiledPackages: CompiledPackages) {

  import Preprocessor._
  import compiledPackages.interface

  val valueTranslator = new ValueTranslator(interface)

  @throws[PreprocessorException]
  def unsafePreprocessCreate(
      templateId: Ref.Identifier,
      argument: Value[Value.ContractId],
  ): (speedy.Command.Create, Set[Value.ContractId]) = {
    val (arg, argCids) = valueTranslator.unsafeTranslateValue(Ast.TTyCon(templateId), argument)
    speedy.Command.Create(templateId, arg) -> argCids
  }

  @throws[PreprocessorException]
  def unsafePreprocessExercise(
      templateId: Ref.Identifier,
      contractId: Value.ContractId,
      choiceId: Ref.ChoiceName,
      argument: Value[Value.ContractId],
  ): (speedy.Command.Exercise, Set[Value.ContractId]) = {
    val choice = handleLookup(interface.lookupChoice(templateId, choiceId)).argBinder._2
    val (arg, argCids) = valueTranslator.unsafeTranslateValue(choice, argument)
    val cids = argCids + contractId
    speedy.Command.Exercise(templateId, SValue.SContractId(contractId), choiceId, arg) -> cids
  }

  @throws[PreprocessorException]
  def unsafePreprocessExerciseByKey(
      templateId: Ref.Identifier,
      contractKey: Value[Value.ContractId],
      choiceId: Ref.ChoiceName,
      argument: Value[Value.ContractId],
  ): (speedy.Command.ExerciseByKey, Set[Value.ContractId]) = {
    val choiceArgType = handleLookup(interface.lookupChoice(templateId, choiceId)).argBinder._2
    val ckTtype = handleLookup(interface.lookupTemplateKey(templateId)).typ
    val (arg, argCids) = valueTranslator.unsafeTranslateValue(choiceArgType, argument)
    val (key, keyCids) = valueTranslator.unsafeTranslateValue(ckTtype, contractKey)
    keyCids.foreach { coid =>
      fail(s"Contract IDs are not supported in contract key of $templateId: $coid")
    }
    speedy.Command.ExerciseByKey(templateId, key, choiceId, arg) -> argCids
  }

  @throws[PreprocessorException]
  def unsafePreprocessCreateAndExercise(
      templateId: Ref.ValueRef,
      createArgument: Value[Value.ContractId],
      choiceId: Ref.ChoiceName,
      choiceArgument: Value[Value.ContractId],
  ): (speedy.Command.CreateAndExercise, Set[Value.ContractId]) = {
    val (createArg, createArgCids) =
      valueTranslator.unsafeTranslateValue(Ast.TTyCon(templateId), createArgument)
    val choiceArgType = handleLookup(interface.lookupChoice(templateId, choiceId)).argBinder._2
    val (choiceArg, choiceArgCids) =
      valueTranslator.unsafeTranslateValue(choiceArgType, choiceArgument)
    speedy.Command
      .CreateAndExercise(
        templateId,
        createArg,
        choiceId,
        choiceArg,
      ) -> (createArgCids | choiceArgCids)
  }

  @throws[PreprocessorException]
  private[preprocessing] def unsafePreprocessLookupByKey(
      templateId: Ref.ValueRef,
      contractKey: Value[Nothing],
  ): speedy.Command.LookupByKey = {
    val ckTtype = handleLookup(interface.lookupTemplateKey(templateId)).typ
    val (key, keyCids) = valueTranslator.unsafeTranslateValue(ckTtype, contractKey)
    keyCids.foreach { coid =>
      fail(s"Contract IDs are not supported in contract keys: $coid")
    }
    speedy.Command.LookupByKey(templateId, key)
  }

  // returns the speedy translation of an LF command together with all the contract IDs contains inside.
  private[preprocessing] def unsafePreprocessCommand(
      cmd: command.Command
  ): (speedy.Command, Set[Value.ContractId]) = {
    cmd match {
      case command.CreateCommand(templateId, argument) =>
        unsafePreprocessCreate(templateId, argument)
      case command.ExerciseCommand(templateId, contractId, choiceId, argument) =>
        unsafePreprocessExercise(templateId, contractId, choiceId, argument)
      case command.ExerciseByKeyCommand(templateId, contractKey, choiceId, argument) =>
        unsafePreprocessExerciseByKey(templateId, contractKey, choiceId, argument)
      case command.CreateAndExerciseCommand(
            templateId,
            createArgument,
            choiceId,
            choiceArgument,
          ) =>
        unsafePreprocessCreateAndExercise(
          templateId,
          createArgument,
          choiceId,
          choiceArgument,
        )
      case command.FetchCommand(templateId, coid) =>
        (speedy.Command.Fetch(templateId, SValue.SContractId(coid)), Set(coid))
      case command.FetchByKeyCommand(templateId, key) =>
        val ckTtype = handleLookup(interface.lookupTemplateKey(templateId)).typ
        val (sKey, cids) = valueTranslator.unsafeTranslateValue(ckTtype, key)
        assert(cids.isEmpty)
        (speedy.Command.FetchByKey(templateId, sKey), Set.empty)
      case command.LookupByKeyCommand(templateId, key) =>
        val ckTtype = handleLookup(interface.lookupTemplateKey(templateId)).typ
        val (sKey, cids) = valueTranslator.unsafeTranslateValue(ckTtype, key)
        assert(cids.isEmpty)
        (speedy.Command.LookupByKey(templateId, sKey), Set.empty)
    }
  }

  @throws[PreprocessorException]
  def unsafePreprocessCommands(
      cmds: ImmArray[command.ApiCommand]
  ): (ImmArray[speedy.Command], Set[Value.ContractId]) = {

    @tailrec
    def go(
        toProcess: FrontStack[command.ApiCommand],
        processed: BackStack[speedy.Command],
        acc: Set[Value.ContractId],
    ): (ImmArray[speedy.Command], Set[Value.ContractId]) = {
      toProcess match {
        case FrontStackCons(cmd, rest) =>
          val (speedyCmd, newCids) = unsafePreprocessCommand(cmd)
          go(rest, processed :+ speedyCmd, acc | newCids)
        case FrontStack() =>
          (processed.toImmArray, acc)
      }
    }

    go(FrontStack(cmds), BackStack.empty, Set.empty)
  }

}
