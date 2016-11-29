package com.octo.nad

import Implicits._
import JavaClassCompiler.global

/**
  * Created by adrien on 24/11/2016.
  */
object JavaClassCompilerExample {

  def main(arguments: Array[String]): Unit = {
    val classSimpleName = "CatTalker"
    val className = s"com.octo.nad.$classSimpleName"
    val classSourceCode =
      s"""
        |package com.octo.nad;
        |
        |import com.octo.nad.Talker;
        |
        |public class $classSimpleName implements Talker {
        |
        |    @Override
        |    public void talk(String message) {
        |       System.out.println("Meow! " + message + "=^..^=");
        |    }
        |
        |}
        |
      """.stripMargin

    implicit val javaClassCompiler: JavaClassCompiler = JavacJavaClassCompiler

    val talker = Talker.builder()
        .byCompiling(className, classSourceCode)
      .build()
    talker.talk("Hello, World! ")
  }

}
