package io.github.banjiaojuhao.sentiment.crawler.spider.zhihu

import io.github.banjiaojuhao.sentiment.crawler.persistence.internal.ZhihuUserTable
import org.jetbrains.exposed.sql.Transaction

data class ZhihuUser(val userId: String, val urlToken: String, val name: String)

// todo refactor to verticle
fun Transaction.upsertUser(userList: Collection<ZhihuUser>) {
    val upsertStmt = this.connection.prepareStatement(
        "insert into ${ZhihuUserTable.tableName}(" +
            "${ZhihuUserTable.userId.name}, " +
            "${ZhihuUserTable.urlToken.name}, " +
            "${ZhihuUserTable.name.name}) " +
            "values(?,?,?) " +
            "on conflict(${ZhihuUserTable.userId.name}) " +
            "do update set " +
            "${ZhihuUserTable.urlToken.name}=excluded.${ZhihuUserTable.urlToken.name}, " +
            "${ZhihuUserTable.name.name}=excluded.${ZhihuUserTable.name.name};")
    for (user in userList) {
        upsertStmt.setString(1, user.userId)
        upsertStmt.setString(2, user.urlToken)
        upsertStmt.setString(3, user.name)
        upsertStmt.addBatch()
    }
    upsertStmt.executeBatch()
}