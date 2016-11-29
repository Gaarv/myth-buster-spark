package com.octo.nad

import scala.util.Try

trait JavaClassCompiler {

  def compile(javaClassSpec: JavaClassSpec[_]): Try[Class[_]]

}

object JavaClassCompiler {

  implicit val global = JavacJavaClassCompiler

}
