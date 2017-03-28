package com.octo.mythbuster.spark.tree

// This trait modelize any type of immutable tree, with some useful methods to work with children
trait TreeNode[Type <: TreeNode[Type]] extends Product {
  self: Type =>

  def children: Seq[Type]

  // Apply f to the children and then to this
  def foreachUp(f: Type => Unit): Unit = {
    children.foreach(_.foreachUp(f))
    f(this)
  }

  // Apply f to this and then to the children
  def foreachDown(f: Type => Unit): Unit = {
    f(this)
    children.foreach(_.foreachDown(f))
  }

  // Transform this and then children were pf is defined or left them unchanged
  def transformUp(pf: PartialFunction[Type, Type]): Type = {
    pf.applyOrElse(mapChildren(_.transformUp(pf)), identity[Type])
  }

  // Transform the childen were pf is defined and then this or left them unchanged
  def transformDown(pf: PartialFunction[Type, Type]): Type = {
    pf.applyOrElse(this, identity[Type]).mapChildren(_.transformDown(pf))
  }

  // Apply f to every children
  def mapChildren(f: Type => Type): Type = copyWithNewChildren(children.map(f))

  // This method needs to be implemented by the concret class in order to explain how to copy a node with new children
  def copyWithNewChildren(children: Seq[Type]): Type

  // Here are two possible implementation (...helpers?) which can be used to implement the method above
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

// This modelize a tree with only two children (which may be suitable for operators, for example)
trait BinaryTreeNode[Type <: TreeNode[Type]] extends TreeNode[Type] {
  self: Type =>

  val leftChild: Type

  val rightChild: Type

  override def copyWithNewChildren(children: Seq[Type]) = copyWithNewChildrenUsingConstructor(children)

  override def children = Seq(leftChild, rightChild)

}

// This modelize a tree with only one child (which my be suitable for postfix operators, for example)
trait UnaryTreeNode[Type <: TreeNode[Type]] extends TreeNode[Type] {
  self: Type =>

  val child: Type

  override def copyWithNewChildren(children: Seq[Type]) = copyWithNewChildrenUsingConstructor(children)

  override def children = Seq(child)

}

// this modelize a tree without any children (which may be suitabe for a literal, for example)
trait LeafTreeNode[Type <: TreeNode[Type]] extends TreeNode[Type] {
  self: Type =>

  def children = Nil

  override def copyWithNewChildren(newChildren: Seq[Type]) = copyWithNewChildrenUsingCopyMethod(children)

}



