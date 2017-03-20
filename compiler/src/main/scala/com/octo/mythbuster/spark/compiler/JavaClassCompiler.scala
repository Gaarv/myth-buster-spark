package com.octo.mythbuster.spark.compiler

import java.io.File.pathSeparator
import java.io.PrintWriter
import java.nio.file.{Files, Path, Paths}

import scala.util.Try

class JavaClassCompilerException(message: String = null, cause: Throwable = null) extends Exception(message, cause)

case class JavaClassSource[T](name: JavaClassName, code: JavaClassCode) {

  def writeCodeTo(folderPath: Path): Try[Path] = {
    val packageName = name.split("\\.").dropRight(1).mkString(".")
    val packageFolderPath = folderPath.resolve(Paths.get(packageName.replace("\\.", pathSeparator)))
    Files.createDirectories(packageFolderPath)

    val simpleName = name.split("\\.").last
    val sourceFilePath = packageFolderPath.resolve(s"$simpleName.java")

    Try {
      val writer = new PrintWriter(Files.newOutputStream(sourceFilePath))
      writer.write(code)
      writer.close()
      sourceFilePath
    }
  }

  def compile()(implicit javaClassCompiler: JavaClassCompiler) = javaClassCompiler.compile(this)

}

trait JavaClassCompiler {

  def compile(javaClassSpec: JavaClassSource[_]): Try[Class[_]]

}

object JavaClassCompiler {

  implicit val global = JavacJavaClassCompiler

}