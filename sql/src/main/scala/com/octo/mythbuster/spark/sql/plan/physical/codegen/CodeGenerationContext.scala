package com.octo.mythbuster.spark.sql.plan.physical.codegen

case class CodeGenerationContext() {

  private var variableCount = 0

  def freshVariableName(): String = {
    val variableName = s"var${variableCount}"
    variableCount = variableCount + 1
    variableName
  }

}
