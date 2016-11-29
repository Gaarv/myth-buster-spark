package com.octo.nad

import java.io.File._
import java.net.URLClassLoader
import java.nio.file.{Files, Path, Paths}

import sys.process._
import scala.util.{Failure, Success, Try}

object JavacJavaClassCompiler extends JavaClassCompiler {

  override def compile(javaClassSpec: JavaClassSpec[_]): Try[Class[_]] = {
    val classFolderPath = Files.createTempDirectory("generated-classes")
    val sourceFolderPath = Files.createTempDirectory("generated-sources")

    for {
      sourceFilePath <- javaClassSpec.writeSourceCodeTo(sourceFolderPath)
      _ <- javac(sourceFilePath, classFolderPath)
      loadedClass <- loadClass(javaClassSpec, classFolderPath)
    } yield loadedClass
  }

  def loadClass(javaClassSpec: JavaClassSpec[_], classFolderPath: Path): Try[Class[_]] = {
    Try {
      val parentClassLoader = getClass.getClassLoader
      val classFolderURL = classFolderPath.toUri.toURL
      val classLoader = new URLClassLoader(Array(classFolderURL), parentClassLoader)
      classLoader.loadClass(javaClassSpec.name)
    }
  }

  def javac(classSourceFilePath: Path, classFolderPath: Path): Try[String] = {
    val output = new StringBuilder
    val appendToOutput: (String => Unit) = line => output.append(s"$line\n")
    val processLogger = ProcessLogger(appendToOutput, appendToOutput)

    Seq("javac", "-verbose", "-cp", javaClassPath().map(_.toString).mkString(pathSeparator), "-d", classFolderPath.toString, classSourceFilePath.toString) ! processLogger match {
      case statusCode if statusCode == 0 => Success(stdout.toString)
      case _ => Failure(new JavaClassCompilationException(output.toString))
    }
  }

  def javaClassPath(): Seq[Path] = {
    Thread.currentThread().getContextClassLoader.asInstanceOf[URLClassLoader].getURLs.map(_.toURI).map(Paths.get)
  }

}
