package cn.banjiaojuhao.sentiment.crawler.spider.tieba

import cn.banjiaojuhao.sentiment.crawler.persistence.internal.TiebaUserTable
import org.jetbrains.exposed.sql.Transaction

data class TiebaUser(val userId: String, val nickname: String)

// todo refactor to verticle
fun Transaction.upsertUsers(userList: Collection<TiebaUser>) {
    this.connection.prepareStatement(
        "insert into ${TiebaUserTable.tableName}(" +
            "${TiebaUserTable.userId.name}, " +
            "${TiebaUserTable.nickname.name}) " +
            "values(?,?) on conflict(${TiebaUserTable.userId.name}) " +
            "do update set " +
            "${TiebaUserTable.nickname.name}=excluded.${TiebaUserTable.nickname.name};")
        .apply {
            for (user in userList) {
                setString(1, user.userId)
                setString(2, user.nickname)
                addBatch()
            }
            executeBatch()
        }
}