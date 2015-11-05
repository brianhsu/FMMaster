package moe.brianhsu.fmmaster.model

import org.squeryl.Schema
import org.squeryl.PrimitiveTypeMode
import org.squeryl.Session
import org.squeryl.SessionFactory
import java.sql.DriverManager
import java.nio.file.Path
import org.squeryl.adapters.SQLiteAdapter
import org.squeryl.KeyedEntity
import org.squeryl.dsl.CompositeKey2

object DatabaseDSL extends PrimitiveTypeMode {

  Class.forName("org.sqlite.JDBC")

  class MetadataSchema(val fieldName: String, val isUnique: Boolean, val fieldType: String, val comment: Option[String])
  class MetadataStore(val sha1: String, val fieldName: String, val content: String)
  class FileIndex(var sha1: String, val filePath: String, 
                  var size: Long, var lastModifiedTime: Long) extends KeyedEntity[CompositeKey2[String, String]] {
    def id = compositeKey(sha1, filePath)
  }
  
  object FMMasterDirDB extends Schema {
    val fileIndex = table[FileIndex]
    val metadataSchema = table[MetadataSchema]
    val metadataStore = table[MetadataStore]

    on(fileIndex)(columns => declare(columns.filePath is indexed("FileIndexFileNameIndex")))
  }

  def using[T](directory: Path)(f: => T) = {
    val session = Session.create(
      DriverManager.getConnection(s"jdbc:sqlite:${directory.toAbsolutePath}/fmmaster.db"),
      new SQLiteAdapter
    )
    val result = transaction(session) { f }
    session.close
    result
  }
}
