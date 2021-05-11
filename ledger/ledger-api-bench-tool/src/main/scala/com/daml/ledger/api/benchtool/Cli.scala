// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.benchtool

import com.daml.ledger.api.v1.ledger_offset.LedgerOffset
import com.daml.ledger.api.v1.value.Identifier
import scopt.{OParser, Read}

import scala.concurrent.duration.Duration

object Cli {
  def config(args: Array[String]): Option[Config] =
    OParser.parse(parser, args, Config.Default)

  private val parser = {
    val builder = OParser.builder[Config]
    import Reads._
    import builder._
    OParser.sequence(
      programName("ledger-api-bench-tool"),
      head("A tool for measuring transaction streaming performance of a ledger."),
      opt[(String, Int)]("endpoint")(endpointRead)
        .abbr("e")
        .text("Ledger API endpoint")
        .valueName("<hostname>:<port>")
        .optional()
        .action { case ((hostname, port), config) =>
          config.copy(ledger = config.ledger.copy(hostname = hostname, port = port))
        },
      opt[Config.StreamConfig]("consume-stream")
        .abbr("s")
        .text(
          s"Stream configuration."
        )
        .valueName(
          "streamType=<transactions|transaction-trees>,name=<streamName>,party=<party>[,begin-offset=<offset>][,end-offset=<offset>][,template-ids=<id1>|<id2>]"
        )
        .action { case (streamConfig, config) => config.copy(streamConfig = Some(streamConfig)) },
      opt[Duration]("log-interval")
        .abbr("r")
        .text("Stream metrics log interval.")
        .action { case (period, config) => config.copy(reportingPeriod = period) },
      help("help").text("Prints this information"),
    )
  }

  private object Reads {
    implicit val streamConfigRead: Read[Config.StreamConfig] =
      Read.mapRead[String, String].map { m =>
        def stringField(fieldName: String): Either[String, String] =
          m.get(fieldName) match {
            case Some(value) => Right(value)
            case None => Left(s"Missing field: '$fieldName'")
          }

        def optionalStringField(fieldName: String): Either[String, Option[String]] =
          Right(m.get(fieldName))

        def offset(stringValue: String): LedgerOffset =
          LedgerOffset.defaultInstance.withAbsolute(stringValue)

        val config = for {
          name <- stringField("name")
          party <- stringField("party")
          streamType <- stringField("streamType").flatMap[String, Config.StreamConfig.StreamType] {
            case "transactions" => Right(Config.StreamConfig.StreamType.Transactions)
            case "transaction-trees" => Right(Config.StreamConfig.StreamType.TransactionTrees)
            case invalid => Left(s"Invalid stream type: $invalid")
          }
          templateIds <- optionalStringField("template-ids")
            .flatMap {
              case Some(ids) => listOfTemplateIds(ids)
              case None => Right(List.empty[Identifier])
            }
          beginOffset <- optionalStringField("begin-offset").map(_.map(offset))
          endOffset <- optionalStringField("end-offset").map(_.map(offset))
        } yield Config.StreamConfig(
          name = name,
          streamType = streamType,
          party = party,
          templateIds = templateIds,
          beginOffset = beginOffset,
          endOffset = endOffset,
        )

        config.fold(error => throw new IllegalArgumentException(error), identity)
      }

    private def listOfTemplateIds(listOfIds: String): Either[String, List[Identifier]] =
      listOfIds
        .split('|')
        .toList
        .map(templateIdFromString)
        .foldLeft[Either[String, List[Identifier]]](Right(List.empty[Identifier])) {
          case (acc, next) =>
            for {
              ids <- acc
              id <- next
            } yield id :: ids
        }

    private def templateIdFromString(fullyQualifiedTemplateId: String): Either[String, Identifier] =
      fullyQualifiedTemplateId
        .split(':')
        .toList match {
        case packageId :: moduleName :: entityName :: Nil =>
          Right(
            Identifier.defaultInstance
              .withEntityName(entityName)
              .withModuleName(moduleName)
              .withPackageId(packageId)
          )
        case _ =>
          Left(s"Invalid template id: $fullyQualifiedTemplateId")
      }

    def endpointRead: Read[(String, Int)] = new Read[(String, Int)] {
      val arity = 1
      val reads: String => (String, Int) = { s: String =>
        splitAddress(s) match {
          case (k, v) => Read.stringRead.reads(k) -> Read.intRead.reads(v)
        }
      }
    }

    private def splitAddress(s: String): (String, String) =
      s.indexOf(':') match {
        case -1 =>
          throw new IllegalArgumentException("Addresses should be specified as `<host>:<port>`")
        case n: Int => (s.slice(0, n), s.slice(n + 1, s.length))
      }
  }

}
