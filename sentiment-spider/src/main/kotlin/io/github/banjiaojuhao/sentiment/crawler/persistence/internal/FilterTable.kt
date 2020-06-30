package io.github.banjiaojuhao.sentiment.crawler.persistence.internal

import org.jetbrains.exposed.dao.IntIdTable

object FilterTable : IntIdTable() {
    override val tableName: String
        get() = "filter_table"

    /**
     *  1: tieba thread
     */
    enum class RecordType(private val value: Int) {
        TiebaThread(1);

        fun toInt() = this.value
    }

    val recordType = integer("record_type")
    val recordId = integer("record_id")

    enum class FilterState(private val value: Int) {
        NotFiltered(0),
        Filtering(1),
        FilterPass(2),
        FilterFail(3);

        fun toInt() = this.value
    }

    val state = integer("state").default(FilterState.NotFiltered.toInt())
    val timestamp = long("timestamp").default(0)

    init {
        uniqueIndex(recordType, recordId)
    }
}