package cn.banjiaojuhao.sentiment.crawler.persistence.external

import cn.banjiaojuhao.sentiment.crawler.config.MyConfig
import cn.banjiaojuhao.sentiment.crawler.config.store
import cn.banjiaojuhao.sentiment.persistence.ArticleTable
import cn.banjiaojuhao.sentiment.persistence.SentenceWordsTable
import cn.banjiaojuhao.sentiment.persistence.WordsTable
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.withContext
import org.jetbrains.exposed.sql.Database
import org.jetbrains.exposed.sql.SchemaUtils
import org.jetbrains.exposed.sql.Transaction
import org.jetbrains.exposed.sql.transactions.transaction
import java.sql.DriverManager
import java.util.concurrent.Executors

object StoreConnection {
    private val db by lazy {
        val url = MyConfig.config[store.mysql.url]
        val dbUser = MyConfig.config[store.mysql.username]
        val dbPassword = MyConfig.config[store.mysql.password]
        Database
            .connect({ DriverManager.getConnection(url, dbUser, dbPassword) })
            .apply {
                transaction(db = this) {
                    SchemaUtils.createMissingTablesAndColumns(
                        ArticleTable, SentenceWordsTable, WordsTable
                    )
                }
                Unit
            }
    }
    private val context = Executors.newSingleThreadExecutor().asCoroutineDispatcher()
    suspend fun <T> execute(task: Transaction.() -> T): T =
        withContext(context) {
            transaction(db = db) {
                task()
            }
        }

    fun close() {
        context.close()
    }
}
