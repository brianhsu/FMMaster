package moe.brianhsu.fmmaster.model

import java.nio.file.Path
import java.io.File

case class BasicFileInfo(filePath: Path, size: Long, lastModifiedTime: Long) {
  def file(implicit dirPath: Path) = new File(filePath.toString)
}

