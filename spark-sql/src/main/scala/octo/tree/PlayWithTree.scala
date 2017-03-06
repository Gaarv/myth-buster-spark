package octo.tree

import scala.reflect.macros.blackbox._

object PlayWithTree {

  trait Node[Type <: Node[Type]] extends Product {
    self: Type =>

    val children: Seq[Type]

    def foreach(f: Type => Unit): Unit = {
      f(this)
      children.foreach(_.foreach(f))
    }

    protected def copyWithChildren(children: Seq[Type]): Type

  }

    def transform(pf: PartialFunction[Type, Type]): Type = transformDown(pf)

    def transformDown(pf: PartialFunction[Type, Type]): Type = {
      apf = pf.applyOrElse(this, identity[Type])
      apf.mapChildren(_.transformDown(pf))6

  }

  case class Word(value: String, child: Option[Word]) extends Node[Word] {

    val children = child.toSeq

    def copyWithChildren(c: Seq[Word]) =

  }

  def main(arguments: Array[String]): Unit = {
    val words = Word("Salut", Some(Word("Les", Some(Word("Les", Some(Word("Amis", None))))))).copy()



    words.foreach({
      case Word(value, _) => println(value)
    })

  }

}
