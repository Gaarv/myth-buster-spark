package com.octo.mythbuster.spark.sql.catalyst.expressions.codegen

import com.octo.nad.{Talker, TalkerException}

import scala.language.implicitConversions
import scala.util.{Failure, Success}

object Implicits {

  class TalkerBuilderWithByCompiling(talkerBuilder: Talker.Builder) {

    def byCompiling(javaClassName: String, javaSourceCode: String)(implicit javaClassCompiler: JavaClassCompiler): Talker.Builder = {
      javaClassCompiler.compile(JavaClassSpec(javaClassName, javaSourceCode)) match {
        case Success(compiledClass) => talkerBuilder.usingClass(compiledClass.asInstanceOf[Class[_ <: Talker]])
        case Failure(e) => throw new TalkerException(s"Unable to do JIT compilation of $javaClassName", e);
      }
    }

  }

  implicit def talkerBuilderWithByCompiling(talkerBuilder: Talker.Builder) = new TalkerBuilderWithByCompiling(talkerBuilder)

}