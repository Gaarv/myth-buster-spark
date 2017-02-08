package com.octo.nad

import org.scalameter.api._
import com.octo.mythbuster.spark.sql.catalyst.expressions.codegen.Implicits._
import com.octo.mythbuster.spark.sql.catalyst.expressions.codegen.JavaClassCompiler.global

/**
  * Created by adrien on 25/11/2016.
  */
class RuntimeCompilerSpec extends BenchSpec {

  val sizes = Gen.range("size")(0, 100, 10)
  val ranges = for {
    size <- sizes
  } yield 0 until size

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
       |       System.out.println("Meow! " + message);
       |    }
       |
        |}
       |
      """.stripMargin
  val talker = Talker.builder()
      .byCompiling(className, classSourceCode)
    .build()

  performance of "CatTalker" in {

    measure method "talk" in {
      using(ranges) in { range =>
        talker.talk("Hello, World!")
      }
    }



  }

}
