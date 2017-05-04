package com.octo.mythbuster.spark

import java.io.InputStream

/**
  * Created by adrien on 5/4/17.
  */
trait AutoCloseImplicits {

  implicit class InputStreamImplicits(inputStream: InputStream) {

    def autoClose[T](block: => T): T = {
      try {
        block
      } finally {
        inputStream.close()
      }
    }

  }

}
