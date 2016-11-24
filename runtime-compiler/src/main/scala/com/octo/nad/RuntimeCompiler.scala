package com.octo.nad

import java.io.{File, PrintWriter}
import java.net.{URL, URLClassLoader}
import java.nio.file.{Files, Path, Paths}

import com.google.common.reflect.Reflection

/**
  * Created by adrien on 24/11/2016.
  */
object RuntimeCompiler {

  def main(arguments: Array[String]): Unit = {
    println("Hello, world! ")
    val generatedClassSimpleName = "CatTalker"
    val generatedClassName = s"com.octo.nad.${generatedClassSimpleName}"
    val classSource =
      s"""
        |package com.octo.nad;
        |
        |import com.octo.nad.Talker;
        |
        |public class ${generatedClassSimpleName} implements Talker {
        |
        |    @Override
        |    public void talk() {
        |       System.out.println("Meow! ");
        |    }
        |
        |}
        |
      """.stripMargin

    val generatedClass = compileClass(generatedClassName, classSource)
    val talker = generatedClass.newInstance().asInstanceOf[Talker]

    talker.talk()
  }

  def compileClass(className: String, classSource: String): Class[_] = {
    val parentClassLoader = getClass.getClassLoader
    val generatedClassesFolder = Files.createTempDirectory("generated-classes")
    val generatedSourcesFolder = Files.createTempDirectory("generated-sources")

    val generatedClassSourcePath = writeClassSource(className, classSource, generatedSourcesFolder)

    javac(className, generatedClassSourcePath, generatedClassesFolder)

    val classFolderURL = generatedClassesFolder.toUri.toURL
    val classLoader = new URLClassLoader(Array(generatedClassesFolder.toUri.toURL), parentClassLoader)
    classLoader.loadClass(className)
  }

  def javac(className: String, classSourceFilePath: Path, classFolderPath: Path): Unit = {
    val process = Runtime.getRuntime.exec(Array("javac", "-cp", currentClassPath(), "-d", classFolderPath.toString, classSourceFilePath.toString))
    process.waitFor()
  }

  def writeClassSource(className: String, classSource : String, sourceFolderPath : Path): Path = {
    val relativePackageFolderPath = Paths.get(Reflection.getPackageName(className).replace('.', '/'))
    val packageFolderPath = sourceFolderPath.resolve(relativePackageFolderPath)
    Files.createDirectories(packageFolderPath)

    val simpleClassName = className.split('.').last

    val classSourcePath = packageFolderPath.resolve(s"${simpleClassName}.java")
    val writer = new PrintWriter(Files.newOutputStream(classSourcePath))
    writer.write(classSource)
    writer.close()

    classSourcePath
  }

  def currentClassPath(): String = {
    val pathSeparator = System.getProperty("path.separator")
    Thread.currentThread().getContextClassLoader.asInstanceOf[URLClassLoader].getURLs.map(_.getPath).mkString(pathSeparator)
  }

}
