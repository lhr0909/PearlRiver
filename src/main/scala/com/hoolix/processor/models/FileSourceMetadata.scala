package com.hoolix.processor.models

/**
  * Hoolix 2017
  * Created by simon on 1/14/17.
  */
case class FileSourceMetadata(fileOffset: FileOffset) extends SourceMetadata {
  type OffsetT = FileOffset

  override val offset: OffsetT = fileOffset

  override def id: String = s"${ offset.fileName }.${ offset.offset }"
}
