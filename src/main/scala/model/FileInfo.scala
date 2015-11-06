package moe.brianhsu.fmmaster.model

import java.nio.file.Path
import java.io.File

case class SHA1FileInfo(basicFileInfo: BasicFileInfo, sha1: String)

case class BasicFileInfo(filePath: String, size: Long, lastModifiedTime: Long) {
  def file(implicit dirPath: Path) = new File(dirPath + File.separator + filePath)
}

