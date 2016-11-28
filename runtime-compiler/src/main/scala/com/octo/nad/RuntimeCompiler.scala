package com.octo.nad

import java.io.File.pathSeparator
import java.io.{File, PrintWriter}
import java.net.{URL, URLClassLoader}
import java.nio.file.{Files, Path, Paths}

import com.google.common.reflect.Reflection

import scala.util.{ Try, Success, Failure }

/**
  * Created by adrien on 24/11/2016.
  */
object RuntimeCompiler {

  def main(arguments: Array[String]): Unit = {
    val classSimpleName = "CatTalker"
    val className = s"com.octo.nad.${classSimpleName}"
    val classSource =
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


    val talker = for {
      runtimeCompiledClass <- JavacJavaClassCompiler.compile(JavaClassSpec(className, classSource))
      runtimeCompiledClassInstance <- Try {
        runtimeCompiledClass.newInstance().asInstanceOf[Talker]
      }
    } yield runtimeCompiledClassInstance

    talker match {
      case Success(talker) => talker.talk("Hello, World! ")
      case Failure(e) => println(s"Unable to do compilation at runtime (${e.getMessage})")

    }
  }

}
