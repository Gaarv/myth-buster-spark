package com.octo.mythbuster.spark.sql.plan.physical.codegen

import com.octo.mythbuster.spark.compiler.JavaCode

case class JavaCodeGenerationContext() {

  private var variableCount = 0

  lazy val iterableVariable = freshVariableName()
  lazy val iteratorVariable = freshVariableName()

  private var globalVariables = Seq[ClassAttribute]()
  private var references = Seq[Object]()

  def freshVariableName(): String = {
    val variableName = s"var${variableCount}"
    variableCount = variableCount + 1
    variableName
  }

  def addReference(reference : Object, typeName : String) : String = {
    val referenceName = freshVariableName()
    references = reference +: references
    val indexReference = references.length - 1
    addClassAttribute(referenceName, typeName, s"($typeName)references[$indexReference]")
    referenceName
  }

  def addClassAttribute(name : String, typeName : String, initCode : JavaCode) =
    globalVariables = ClassAttribute(name, typeName, initCode) +: globalVariables

  def generateInitCode = globalVariables.map(_.generateInitCode).mkString("\n")

  def generateAttributesDeclarationCode = globalVariables.map(_.generateDeclarationCode).mkString("\n")

  def getReferencesAsArray = references.toArray

}

case class ClassAttribute(name : String, typeName : String, initCode : JavaCode) {

  def generateInitCode = s"$name = $initCode;"

  def generateDeclarationCode = s"private $typeName $name;"

}
