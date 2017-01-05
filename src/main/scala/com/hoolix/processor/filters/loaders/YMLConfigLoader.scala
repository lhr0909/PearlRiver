package com.hoolix.processor.filters.loaders

import com.hoolix.processor.filters.Filter

/**
  * Created by peiyuchao on 2017/1/3.
  */

case class YMLConfigLoader() {
  override def load(): Seq[Filter] = {
    // load from *.yml
  }
}