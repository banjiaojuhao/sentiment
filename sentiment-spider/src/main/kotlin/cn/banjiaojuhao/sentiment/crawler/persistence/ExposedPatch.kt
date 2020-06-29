package cn.banjiaojuhao.sentiment.crawler.persistence

import org.jetbrains.exposed.dao.IntIdTable
import org.jetbrains.exposed.sql.Op
import org.jetbrains.exposed.sql.SqlExpressionBuilder
import org.jetbrains.exposed.sql.select
import org.jetbrains.exposed.sql.statements.UpdateStatement
import org.jetbrains.exposed.sql.update

fun <T : IntIdTable> T.updatePatch(where: (SqlExpressionBuilder.() -> Op<Boolean>)? = null, limit: Int? = null, body: T.(UpdateStatement) -> Unit): Int {
    val newWhere = when {
        limit == null -> {
            where
        }
        where == null -> {
            null
        }
        else -> {
            {
                this@updatePatch.id inSubQuery (
                    this@updatePatch.slice(id).select(where.let { SqlExpressionBuilder.it() }).limit(limit)
                    )
            }
        }
    }
    return this.update(newWhere, null, body)
}