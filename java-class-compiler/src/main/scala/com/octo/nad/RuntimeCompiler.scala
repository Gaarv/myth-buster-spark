package com.octo.nad

import java.io.File.pathSeparator
import java.io.{File, PrintWriter}
import java.net.{URL, URLClassLoader}
import java.nio.file.{Files, Path, Paths}

import com.google.common.reflect.Reflection

import scala.util.{ Try, Success, Failure }

import Implicits._
import JavaClassCompiler.global

/**
  * Created by adrien on 24/11/2016.
  */
object RuntimeCompiler {

  def main(arguments: Array[String]): Unit = {
    val classSimpleName = "CatTalker"
    val className = s"com.octo.nad.${classSimpleName}"
    val classSourceCode =
      s"""
        |package com.octo.nad;
        |
        |import com.octo.nad.Talker;
        |
        |public class ${classSimpleName} implements Talker {
        |
        |    @Override
        |    public void talk(String message) {
        |       System.out.println("Meow! " + message);
        |    }
        |
        |}
        |
      """.stripMargin


    /*val maybeTalker = for {
      runtimeCompiledClass <- JavacJavaClassCompiler.compile(JavaClassSpec(className, classSourceCode))
      runtimeCompiledClassInstance <- Try {
        runtimeCompiledClass.newInstance().asInstanceOf[Talker]
      }
    } yield runtimeCompiledClassInstance

    maybeTalker match {
      case Success(talker) => talker.talk("Hello, World! ")
      case Failure(e) => println(s"Unable to do compilation at runtime (${e.getMessage})")

    }*/

    implicit val javaClassCompiler: JavaClassCompiler = JavacJavaClassCompiler

    val talker = Talker.builder()
        .byCompiling(className, classSourceCode)
      .build()
    talker.talk("Message")
  }

}
