package com.daml
package lf
package archive

import java.nio.file.Paths

private[lf] object StandardPackagesEncoder{

  val all =  language.StandardPackages.all.map { case (pkgId, pkg) =>
    val archive = Encode.encodeArchive((pkgId, pkg), pkg.languageVersion)
    println(archive.getHash)
    (pkgId: String) -> archive.toByteArray
  }.toList

}
