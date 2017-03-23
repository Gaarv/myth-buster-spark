package com.octo.mythbuster.spark

import java.util.concurrent.Callable

import com.google.common.cache.{Cache => GuavaCache, CacheBuilder => GuavaCacheBuilder}

trait Caching[Key <: AnyRef, Value <: AnyRef] {

  case class Cache[Key, Value](cache: GuavaCache[Key, Value]) {

    def get(key: Key)(loader: Key => Value): Value = {
      cache.get(key, { () => loader(key) })
    }

  }

  val cache: Cache[Key, Value] = Cache(GuavaCacheBuilder.newBuilder().build[Key, Value]())

}
