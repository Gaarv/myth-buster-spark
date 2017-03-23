package com.octo.mythbuster.spark.compiler

import com.octo.mythbuster.spark.UnitSpec
import com.octo.mythbuster.spark.compiler.sample.Talker

import scala.util.{ Failure, Success }

class JavaCompilerSpec extends UnitSpec {

  val badClassCode: JavaClassCode = "Rond comme un balon, plus jaune qu'un citron... C'est Pac-Man!"

  "Bad class source compilation" should "fail" in {
    JavaClassSource("bad.PacMan", badClassCode).compile() shouldBe a[Failure[_]]
  }

  val classCode: JavaClassCode =
    """package com.octo.mythbuster.spark.compiler.sample;
      |
      |public class TalkerImpl implements Talker {
      |
      | public TalkerImpl() {
      |   super();
      | }
      |
      | @Override
      | public String talk() {
      |   return "Hello, World! ";
      | }
      |
      |}
      |
    """.stripMargin

  val classSource = JavaClassSource("com.octo.mythbuster.spark.compiler.sample.TalkerImpl", classCode)

  "The class source" should "be compiled" in {
    classSource.compile() match {
      case Failure(e) => fail(e)
      case Success(compiledClass) => {
        val talker = compiledClass.newInstance().asInstanceOf[Talker]
        talker.talk() should be("Hello, World! ")
      }

    }
  }

}
