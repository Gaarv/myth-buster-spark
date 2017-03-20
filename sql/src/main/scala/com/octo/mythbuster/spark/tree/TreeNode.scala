package com.octo.mythbuster.spark.tree

trait TreeNode[Type <: TreeNode[Type]] extends Product {
  self: Type =>

  def children: Seq[Type]

  def foreachUp(f: Type => Unit): Unit = {
    children.foreach(_.foreachUp(f))
    f(this)
  }

  def foreachDown(f: Type => Unit): Unit = {
    f(this)
    children.foreach(_.foreachDown(f))
  }

  def transformUp(pf: PartialFunction[Type, Type]): Type = {
    pf.applyOrElse(mapChildren(_.transformUp(pf)), identity[Type])
  }

  def transformDown(pf: PartialFunction[Type, Type]): Type = {
    pf.applyOrElse(this, identity[Type]).mapChildren(_.transformDown(pf))
  }

  def mapChildren(f: Type => Type): Type = copyWithNewChildren(children.map(f))

  def copyWithNewChildren(children: Seq[Type]): Type

  protected def copyWithNewChildrenUsingConstructor(newChildren: Seq[Type], expectedChildCount: Int = -1) : Type = {
    if (expectedChildCount >= 0 && newChildren.length > expectedChildCount) throw new IllegalArgumentException("There is to much children")

    val constructorArguments = newChildren ++ productIterator.toSeq.drop(newChildren.length)

    getClass.getConstructors.filter(_.getParameters.size == productArity).toSeq match {
      case Seq(constructor) => constructor.newInstance(constructorArguments.map(_.asInstanceOf[Object]): _*).asInstanceOf[Type]
      case _ => throw new IllegalArgumentException(s"Unable to find a suitable constructor in order to copy ${this} with new children")
    }
  }

  protected def copyWithNewChildrenUsingCopyMethod(newChildren: Seq[Type]): Type = {
    (newChildren, getClass.getMethods.filter(_.getName eq "copy").find(_.getParameterCount == productArity)) match {
      case (Seq(), Some(copyMethod)) => copyMethod.invoke(this.asInstanceOf[AnyRef], productIterator.map(_.asInstanceOf[AnyRef]).toSeq: _*).asInstanceOf[Type]
      case (_, None) => throw new IllegalArgumentException(s"Unable to find copy method of ${this}")
      case (_, Some(_)) => throw new IllegalArgumentException(s"There must be no children in order to copy ${this}")
    }
  }

}

trait BinaryTreeNode[Type <: TreeNode[Type]] extends TreeNode[Type] {
  self: Type =>

  val leftChild: Type

  val rightChild: Type

  override def copyWithNewChildren(children: Seq[Type]) = copyWithNewChildrenUsingConstructor(children)

  override def children = Seq(leftChild, rightChild)

}

trait UnaryTreeNode[Type <: TreeNode[Type]] extends TreeNode[Type] {
  self: Type =>

  val child: Type

  override def copyWithNewChildren(children: Seq[Type]) = copyWithNewChildrenUsingConstructor(children)

  override def children = Seq(child)

}

trait LeafTreeNode[Type <: TreeNode[Type]] extends TreeNode[Type] {
  self: Type =>

  def children = Nil

  override def copyWithNewChildren(newChildren: Seq[Type]) = copyWithNewChildrenUsingCopyMethod(children)

}



