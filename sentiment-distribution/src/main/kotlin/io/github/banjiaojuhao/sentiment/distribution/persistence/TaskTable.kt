package io.github.banjiaojuhao.sentiment.distribution.persistence

import org.jetbrains.exposed.dao.IntIdTable

object TaskTable : IntIdTable() {
    override val tableName: String
        get() = "task_status"

    enum class TYPE {
        SENTIMENT_WORD;

        fun toInt() = when (this) {
            SENTIMENT_WORD -> 1
        }
    }

    val type = integer("type")
    val rid = integer("raw_id")

    enum class STATUS {
        NOT_PROCESS, PROCESSING, PROCESSED, ERROR;

        fun toInt() = when (this) {
            NOT_PROCESS -> 1
            PROCESSING -> 2
            PROCESSED -> 3
            ERROR -> 4
        }
    }

    val status = integer("status")
    val timestamp = long("timestamp")

    init {
        uniqueIndex(type, rid)
    }
}

object ErrorTable : IntIdTable() {
    override val tableName: String
        get() = "error_table"
    val tid = integer("task_id").references(TaskTable.id)
    val msg = text("error_msg")
    val timestamp = long("timestamp")
}