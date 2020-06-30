package io.github.banjiaojuhao.sentiment.crawler.spider.weibo

import io.github.banjiaojuhao.sentiment.crawler.persistence.internal.WeiboUserTable
import org.jetbrains.exposed.sql.Transaction

internal data class WeiboUser(val userId: String, val userName: String)

// todo refactor to verticle
internal fun Transaction.upsertUser(userList: Collection<WeiboUser>) {
    this.connection.prepareStatement(
        "insert into ${WeiboUserTable.tableName}(" +
            "${WeiboUserTable.userId.name}, " +
            "${WeiboUserTable.userName.name}) " +
            "values(?,?) on conflict(${WeiboUserTable.userId.name}) " +
            "do update set " +
            "${WeiboUserTable.userName.name}=excluded.${WeiboUserTable.userName.name};")
        .apply {
            for (user in userList) {
                setString(1, user.userId)
                setString(2, user.userName)
                addBatch()
            }
            executeBatch()
        }
}