package cn.banjiaojuhao.sentiment.crawler.persistence.internal

import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.withContext
import org.jetbrains.exposed.sql.Database
import org.jetbrains.exposed.sql.SchemaUtils
import org.jetbrains.exposed.sql.Transaction
import org.jetbrains.exposed.sql.transactions.transaction
import java.sql.Connection.TRANSACTION_SERIALIZABLE
import java.util.concurrent.Executors

object CacheConnection {
    private val db by lazy {
        val db = Database.connect("jdbc:sqlite:data.db", "org.sqlite.JDBC")
        transaction(db = db, transactionIsolation = TRANSACTION_SERIALIZABLE,
            repetitionAttempts = 3) {
            SchemaUtils.createMissingTablesAndColumns(
                ZhihuQuestionTable, ZhihuAnswerTable, ZhihuCommentTable, ZhihuUserTable,
                TiebaThreadTable, TiebaPostTable, TiebaCommentTable, TiebaUserTable,
                WeiboPostTable, WeiboCommentTable, WeiboSubCommentTable, WeiboUserTable,
                FilterTable
            )
        }
        db
    }
    private val context = Executors.newSingleThreadExecutor().asCoroutineDispatcher()
    suspend fun <T> execute(task: Transaction.() -> T): T =
        withContext(context) {
            transaction(db = db, transactionIsolation = TRANSACTION_SERIALIZABLE,
                repetitionAttempts = 3) {
                task()
            }
        }

    fun close() {
        context.close()
    }
}
