package com.octo.nad

import java.io.File.pathSeparator
import java.io.PrintWriter
import java.nio.file.{Files, Path, Paths}

import scala.util.Try

case class JavaClassSpec[T](name: String, sourceCode: String) {

  def writeSourceCodeTo(folderPath: Path): Try[Path] = {
    val packageName = name.split("\\.").dropRight(1).mkString(".")
    val packageFolderPath = folderPath.resolve(Paths.get(packageName.replace("\\.", pathSeparator)))
    Files.createDirectories(packageFolderPath)

    val simpleName = name.split("\\.").last
    val sourceFilePath = packageFolderPath.resolve(s"$simpleName.java")

    Try {
      val writer = new PrintWriter(Files.newOutputStream(sourceFilePath))
      writer.write(sourceCode)
      writer.close()
      sourceFilePath
    }
  }

}
