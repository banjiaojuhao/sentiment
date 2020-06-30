package io.github.banjiaojuhao.sentiment.crawler.persistence.internal

import org.jetbrains.exposed.dao.IntIdTable
import org.jetbrains.exposed.sql.*

open class UpdateInfoTable(var refreshInterval: Long = 0L,
                           var fetchInterval: Long = 0L) : IntIdTable() {
    val triedUpdateTime = long("_update_time").default(0)
    val updated = bool("_updated").default(false)

    fun reset(additionalCondition: (SqlExpressionBuilder.() -> Expression<Boolean>)? = null) {
        update({
            (updated eq false) and
                (additionalCondition?.let { SqlExpressionBuilder.it() } ?: Op.TRUE)
        }) {
            it[this.triedUpdateTime] = 0
        }
    }

    fun refresh(additionalCondition: (SqlExpressionBuilder.() -> Expression<Boolean>)? = null) {
        update({
            (triedUpdateTime less System.currentTimeMillis() - refreshInterval) and
                (additionalCondition?.let { SqlExpressionBuilder.it() } ?: Op.TRUE)
        }) {
            it[this.updated] = false
        }
    }

    fun loadTask(vararg columns: Expression<*>,
                 additionalCondition: (SqlExpressionBuilder.() -> Op<Boolean>)? = null): Query {
        val updateTag = System.currentTimeMillis()
        update({
            (updated eq false) and
                (triedUpdateTime less updateTag - fetchInterval) and
                (additionalCondition?.let { SqlExpressionBuilder.it() } ?: Op.TRUE)
        }) {
            it[this.triedUpdateTime] = updateTag
        }
        return if (columns.isEmpty()) {
            select {
                triedUpdateTime eq updateTag
            }
        } else {
            slice(*columns).select {
                triedUpdateTime eq updateTag
            }
        }
    }

    fun finished(additionalCondition: (SqlExpressionBuilder.() -> Op<Boolean>)) {
        update({
            additionalCondition()
        }) {
            it[this.updated] = true
        }
    }
}