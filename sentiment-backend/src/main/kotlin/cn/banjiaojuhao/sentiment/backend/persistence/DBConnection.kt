package cn.banjiaojuhao.sentiment.backend.persistence

import cn.banjiaojuhao.sentiment.backend.config.MyConfig
import cn.banjiaojuhao.sentiment.backend.config.store
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.withContext
import org.jetbrains.exposed.sql.Database
import org.jetbrains.exposed.sql.SchemaUtils
import org.jetbrains.exposed.sql.Transaction
import org.jetbrains.exposed.sql.insertIgnore
import org.jetbrains.exposed.sql.transactions.transaction
import java.sql.Connection.TRANSACTION_SERIALIZABLE
import java.sql.DriverManager
import java.util.concurrent.Executors

object DBConnection {
    private val db: Database by lazy {
        val url = "jdbc:mysql://localhost:3306/sentiment"
        val dbUser = MyConfig.config[store.mysql.username]
        val dbPassword = MyConfig.config[store.mysql.password]
//        Database.connect("jdbc:sqlite:data.db", "org.sqlite.JDBC").apply {
        Database
            .connect({ DriverManager.getConnection(url, dbUser, dbPassword) })
            .apply {
                transaction(db = this) {
                    SchemaUtils.createMissingTablesAndColumns(
                        ArticleTable, WhiteListTable,
                        UserLoginInfo, UserBasicInfo, LoginSession
                    )
                    UserLoginInfo.insertIgnore {
                        it[username] = "root"
                        it[password] = "firstpwd"
                    }
                    UserLoginInfo.insertIgnore {
                        it[username] = "first_user"
                        it[password] = "secondpwd"
                    }
                    UserBasicInfo.insertIgnore {
                        it[roles] = "root"
                        it[department] = "master"
                        it[phone] = "10086"
                        it[username] = "root"
                    }
                    UserBasicInfo.insertIgnore {
                        it[roles] = "user"
                        it[department] = "canteen"
                        it[phone] = "10086"
                        it[username] = "first_user"
                    }
                }
                Unit
            }
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
