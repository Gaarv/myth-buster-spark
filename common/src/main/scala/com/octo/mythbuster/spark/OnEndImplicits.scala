package com.octo.mythbuster.spark

/**
  * Created by adrien on 5/4/17.
  */
trait OnEndImplicits {

  implicit class IteratorImplicits[T](iterator: Iterator[T]) {

    def onEnd(block: => Unit): Iterator[T] = {
      new Iterator[T] {
        def hasNext = {
          val b = iterator.hasNext
          if (!b) block
          b
        }

        def next() = iterator.next()

      }
    }

  }

}
