// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.codegen

import java.io.File
import java.nio.file.Files

import com.daml.bazeltools.BazelRunfiles
import com.daml.lf.archive.DarReader
import com.daml.lf.data.ImmArray.ImmArraySeq
import com.daml.lf.data.Ref.{DottedName, ModuleName, PackageId, QualifiedName}
import com.daml.lf.iface.{DefDataType, Interface, InterfaceType, Record}
import com.daml.lf.codegen.backend.java.JavaBackend
import com.daml.lf.codegen.conf.Conf
import org.scalatest.matchers.should.Matchers
import org.scalatest.flatspec.AnyFlatSpec

class CodeGenRunnerTests extends AnyFlatSpec with Matchers with BazelRunfiles {

  behavior of "collectDamlLfInterfaces"

  def path(p: String) = new File(p).getAbsoluteFile.toPath

  val testDar = path(rlocation("language-support/java/codegen/test-daml.dar"))
  val dar = DarReader().readArchiveFromFile(testDar.toFile).get

  val dummyOutputDir = Files.createTempDirectory("codegen")

  it should "always use JavaBackend, which is currently hardcoded" in {
    CodeGenRunner.backend should be theSameInstanceAs JavaBackend
  }

  it should "read interfaces from a single DAR file without a prefix" in {

    val conf = Conf(
      Map(testDar -> None),
      dummyOutputDir,
    )

    val (interfaces, pkgPrefixes) = CodeGenRunner.collectDamlLfInterfaces(conf)

    assert(interfaces.length == 20)
    assert(pkgPrefixes == Map.empty)
  }

  it should "read interfaces from a single DAR file with a prefix" in {

    val conf = Conf(
      Map(testDar -> Some("PREFIX")),
      dummyOutputDir,
    )

    val (interfaces, pkgPrefixes) = CodeGenRunner.collectDamlLfInterfaces(conf)

    assert(interfaces.map(_.packageId).length == dar.all.length)
    assert(pkgPrefixes.size == dar.all.length)
    assert(pkgPrefixes.values.forall(_ == "PREFIX"))
  }

  behavior of "detectModuleCollisions"

  def interface(pkgId: String, metadata: modNames: String*) = {
    val dummyType = InterfaceType.Normal(DefDataType(ImmArraySeq.empty, Record(ImmArraySeq.empty)))
    Interface(
      PackageId.assertFromString(pkgId),
      None,
      modNames.view.map(n => QualifiedName(ModuleName.assertFromString(n), DottedName.assertFromString("Dummy")) -> dummyType).toMap)
  }

  it should "succeed if there are no collisions" in {
    CodeGenRunner.detectModuleCollisions(Map.empty, Seq(interface("pkg1", "A", "A.B"), interface("pkg2", "B", "A.B.C"))) shouldBe ()
  }

  it should "fail if there is a collision" in {
    assertThrows[IllegalArgumentException] {
      CodeGenRunner.detectModuleCollisions(Map.empty, Seq(interface("pkg1", "A"), interface("pkg2", "A")))
    }
  }

  it should "fail if there is a collision caused by prefixing" in {
    assertThrows[IllegalArgumentException] {
      CodeGenRunner.detectModuleCollisions(Map(PackageId.assertFromString("pkg2") -> "A"), Seq(interface("pkg1", "A.B"), interface("pkg2", "B")))
    }
  }

  it should "succeed if collision is resolved by prefixing" in {
    CodeGenRunner.detectModuleCollisions(Map(PackageId.assertFromString("pkg2") -> "Pkg2"), Seq(interface("pkg1", "A"), interface("pkg2", "A"))) shouldBe ()
  }

  behavior of "resolvePackagePrefixes"

  def
}
