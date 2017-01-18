package com.hoolix.processor.models

/**
  * Created by peiyuchao on 2017/1/18.
  */
case class DummySourceMetadata() extends SourceMetadata {
  // does not know where am I coming from
  override type OffsetT = Any
  override val offset: OffsetT = None
  override def id: String = ???
}


//case class FileSourceMetadata(fileOffset: FileOffset) extends SourceMetadata {
//  type OffsetT = FileOffset
//
//  override val offset: OffsetT = fileOffset
//
//  override def id: String = s"${ offset.fileName }.${ offset.offset }"
//}