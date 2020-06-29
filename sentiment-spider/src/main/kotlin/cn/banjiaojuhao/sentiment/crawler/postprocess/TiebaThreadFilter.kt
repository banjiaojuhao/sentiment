package cn.banjiaojuhao.sentiment.crawler.postprocess

import cn.banjiaojuhao.sentiment.crawler.config.MyConfig
import cn.banjiaojuhao.sentiment.crawler.config.getInterval
import cn.banjiaojuhao.sentiment.crawler.config.tieba
import cn.banjiaojuhao.sentiment.crawler.persistence.internal.CacheConnection
import cn.banjiaojuhao.sentiment.crawler.persistence.internal.FilterTable
import cn.banjiaojuhao.sentiment.crawler.persistence.internal.TiebaThreadTable
import kotlinx.coroutines.delay
import org.jetbrains.exposed.sql.and
import org.jetbrains.exposed.sql.not
import org.jetbrains.exposed.sql.select
import java.util.regex.Pattern

// filter out unrelated posts in tieba
object TiebaThreadFilter {
    private val patternEn = Pattern.compile("((hit)|(HIT)).*?((sz)|(SZ))")
    private val patternZh = Pattern.compile("哈.*?工.*?深")

    private val runInterval = MyConfig.config.getInterval(tieba.thread.filter.run_interval)
    suspend fun run() {
        while (true) {
            if (filter() == 0) {
                delay(runInterval)
            }
        }
    }

    private suspend fun filter(): Int {
        var rowsAffected = 0
        CacheConnection.execute {
            TiebaThreadTable.slice(
                TiebaThreadTable.id,
                TiebaThreadTable.title
            ).select {
                not(TiebaThreadTable.id inSubQuery
                    (FilterTable.slice(FilterTable.recordId).select {
                        (FilterTable.recordType eq FilterTable.RecordType.TiebaThread.toInt()) and
                            (FilterTable.state neq FilterTable.FilterState.NotFiltered.toInt())
                    })
                )
            }.limit(1000).toList()
        }.map {
            val title = it[TiebaThreadTable.title]
            val filterPass = title.contains("哈深") ||
                patternEn.matcher(title).find() ||
                patternZh.matcher(title).find()

            val state = if (filterPass) {
                FilterTable.FilterState.FilterPass.toInt()
            } else {
                FilterTable.FilterState.FilterFail.toInt()
            }
            Pair(it[TiebaThreadTable.id].value, state)
        }.let {
            CacheConnection.execute {
                this.connection.prepareStatement(
                    "insert into ${FilterTable.tableName}(" +
                        "${FilterTable.recordType.name}, " +
                        "${FilterTable.recordId.name}, " +
                        "${FilterTable.state.name}, " +
                        "${FilterTable.timestamp.name}) " +
                        "values(?,?,?,?) " +
                        "on conflict(${FilterTable.recordType.name}, ${FilterTable.recordId.name}) " +
                        "do update set " +
                        "${FilterTable.state.name}=excluded.${FilterTable.state.name}, " +
                        "${FilterTable.timestamp.name}=excluded.${FilterTable.timestamp.name};")
                    .apply {
                        it.forEach {
                            this.setInt(1, FilterTable.RecordType.TiebaThread.toInt())
                            this.setInt(2, it.first)
                            this.setInt(3, it.second)
                            this.setLong(4, System.currentTimeMillis())
                            this.addBatch()
                        }
                        rowsAffected = this.executeBatch().sum()
                    }
            }
        }
        return rowsAffected
    }
}