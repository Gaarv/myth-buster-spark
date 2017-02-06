package com.octo.mythbuster.volcano

object VolcanoExample {

  def main(arguments: Array[String]): Unit = {
    val animals = Seq(
      animal(1, "Elephant", 100, 75, 2),
      animal(2, "Dog", 10, 10, 1),
      animal(3, "Ant", 1, 1, 3),
      animal(4, "Cat", 10, 10, 2)
    )

    val zoos = Seq(
      zoo(1, "La Fistinière"),
      zoo(2, "La Chapelle Fistine"),
      zoo(3, "Islamiste")
    )



    val volcanoIterator = new Filter(new CartesianProduct(new SeqScan(animals.iterator), new SeqScan(zoos.iterator)), (row) => row("animalZooId") == row("zooId"))

    //val scan = new SeqScan(animals.iterator)
    //val filter = new Filter(scan, (row) => row("name") == "Cat")
    //val projection = new Projection(filter, Seq("weight"))

    //val volcanoIterator = projection


    /*
            Projection
              |
              |
              v
            Filter
              |
              |
          +--Join---+
          |         |
          |         |
          v         v
        Scan       Scan

     */

    while (volcanoIterator.next()) {
      println(volcanoIterator.fetch())
    }


  }

  /*
     (a, b) x (c, d) = ((a, c), (a, d), (b, c), (b, d))
   */

  class CartesianProduct(leftChild: VolcanoIterator, rightChild: VolcanoIterator) extends VolcanoIterator {

    var rightChildCacheHasBeenSet = false
    val rightChildCache: scala.collection.mutable.Buffer[Map[String, Any]] = scala.collection.mutable.Buffer()

    var rightChildCacheIterator: Iterator[Map[String, Any]] = null
    var rightToBeFetched: Map[String, Any] = null

    override def next(): Boolean = {
      //println("--> next")

      if (!rightChildCacheHasBeenSet) {
        if (!leftChild.next()) {
          return false
        }

        while (rightChild.next()) {
          rightChildCache.append(rightChild.fetch())
        }
        //println(rightChildCache)
        rightChildCacheHasBeenSet = true
        rightChildCacheIterator = rightChildCache.iterator
        //println(rightChildCacheIterator)
      }

      if (!rightChildCacheIterator.hasNext) {
        rightChildCacheIterator = rightChildCache.iterator

        rightToBeFetched = rightChildCacheIterator.next()
        leftChild.next()
      } else {
        rightToBeFetched = rightChildCacheIterator.next()
        true
      }
    }

    override def fetch(): Map[String, Any] = {
      //println("--> fetch")
      //println("rightChildCacheIterator=" + rightChildCacheIterator)
      leftChild.fetch() ++ rightToBeFetched
    }

  }

  class Projection(child: VolcanoIterator, keys: Seq[String]) extends VolcanoIterator {

    override def next(): Boolean = {
      child.next()
    }

    override def fetch() = {
      child.fetch().filterKeys(keys.contains(_))
    }

  }

  class SeqScan(iterator: Iterator[Map[String, Any]]) extends VolcanoIterator {

    var toBeFetched: Map[String, Any] = null

    override def next() = {
      val b = iterator.hasNext
      if (b) {
        toBeFetched = iterator.next()
      }
      b
    }

    override def fetch() = {
      toBeFetched
    }

  }

  class Filter(child: VolcanoIterator, predicate: Map[String, Any] => Boolean) extends VolcanoIterator {

    override def next(): Boolean = {
      var found = false
      while (!found && child.next()) {
        found = predicate(child.fetch())
      }
      found
    }

    override def fetch(): Map[String, Any] = {
      child.fetch()
    }

    /*def predicate(row: Map[String, Any]): Boolean = {
      row.get(key).map(_.asInstanceOf[Int]).map(_ > threshold).getOrElse(false)
    }*/


  }

  trait VolcanoIterator {

    def next(): Boolean

    def fetch(): Map[String, Any]

  }

  def animal(id: Int, name: String, weight: Int, size: Int, zooId: Int): Map[String, Any] = {
    Map(
      "animalId" -> id,
      "animalName" -> name,
      "animalWeight" -> weight,
      "animalSize" -> size,
      "animalZooId" -> zooId
    )
  }

  def zoo(id: Int, name: String): Map[String, Any] = {
    Map(
      "zooId" -> id,
      "zooName" -> name
    )
  }

}
