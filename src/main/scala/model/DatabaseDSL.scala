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
import javax.sql.DataSource

object DatabaseDSL extends PrimitiveTypeMode {

  Class.forName("org.sqlite.JDBC")

  class MetadataSchema(val fieldName: String, val isUnique: Boolean, val fieldType: String, val comment: Option[String])
  class MetadataStore(val sha1: String, val fieldName: String, val content: String)
  class DeletedIndex(val filePath: String, val sha1: String, val deletedTime: Long)
  class FileIndex(var sha1: String, val filePath: String, 
                  var size: Long, var lastModifiedTime: Long) extends KeyedEntity[CompositeKey2[String, String]] {
    def id = compositeKey(sha1, filePath)
  }
  
  object FMMasterDirDB extends Schema {
    val fileIndex = table[FileIndex]
    val deletedIndex = table[DeletedIndex]
    val metadataSchema = table[MetadataSchema]
    val metadataStore = table[MetadataStore]

    on(fileIndex)(columns => declare(columns.filePath is indexed("FileIndexFileNameIndex")))
    on(deletedIndex)(columns => declare(columns.filePath is indexed("DeletedIndexFileNameIndex")))
  }

  def using[T](dataSource: DataSource)(f: => T) = {
    val session = Session.create(dataSource.getConnection, new SQLiteAdapter)
    val result = transaction(session) { f }
    session.close
    result
  }

  def databaseFile(directory: Path) = s"${directory.toAbsolutePath}/.fmmaster.db"

  def databaseFiles(directory: Path) = Set(
    s"${directory.toAbsolutePath}/.fmmaster.db",
    s"${directory.toAbsolutePath}/.fmmaster.db-journal"
  )

  def createConnectionPool(directory: Path): DataSource = {
    import org.apache.commons.pool2.impl.GenericObjectPool
    import org.apache.commons.dbcp2.DriverManagerConnectionFactory
    import org.apache.commons.dbcp2.PoolableConnectionFactory
    import org.apache.commons.dbcp2.PoolingDataSource
    val jdbcURL = s"jdbc:sqlite:${databaseFile(directory)}"
    val connectionFactory = new DriverManagerConnectionFactory(jdbcURL, null)
    val poolableConnectionFactory = new PoolableConnectionFactory(connectionFactory, null)
    val connectionPool = new GenericObjectPool(poolableConnectionFactory)
    poolableConnectionFactory.setPool(connectionPool)
    new PoolingDataSource(connectionPool)
  }
}
