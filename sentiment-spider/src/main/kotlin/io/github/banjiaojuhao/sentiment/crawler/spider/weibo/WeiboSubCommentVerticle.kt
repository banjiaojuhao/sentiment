package io.github.banjiaojuhao.sentiment.crawler.spider.weibo

import io.github.banjiaojuhao.sentiment.crawler.config.EventbusAddress
import io.github.banjiaojuhao.sentiment.crawler.persistence.internal.CacheConnection
import io.github.banjiaojuhao.sentiment.crawler.persistence.internal.WeiboCommentTable
import io.github.banjiaojuhao.sentiment.crawler.persistence.internal.WeiboSubCommentTable
import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.annotation.JSONField
import io.vertx.core.json.JsonObject
import io.vertx.kotlin.core.eventbus.requestAwait
import io.vertx.kotlin.core.json.jsonObjectOf
import io.vertx.kotlin.coroutines.CoroutineVerticle
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import org.jetbrains.exposed.sql.Transaction
import org.jsoup.Jsoup
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import java.util.*

class WeiboSubCommentVerticle : CoroutineVerticle() {
    private data class RespJson(
        val `data`: List<SubComment>,
        val max: Int,
        val max_id: Long,
        val max_id_type: Int,
        val ok: Int,
        val total_number: Int
    )

    private data class SubComment(
        val created_at: String,
        @JSONField(deserialize = false)
        var timestamp: Long = 0L,
        val floor_number: Int,
        val id: String,
        val like_count: Int,
        val mid: String,
        val rootid: String,
        val rootidstr: String,
        val source: String,
        val text: String,
        @JSONField(deserialize = false)
        var parsedContent: String = "",
        val user: User
    )

    private data class User(
        val avatar_hd: String,
        val cover_image_phone: String,
        val description: String,
        val follow_count: Int,
        val followers_count: Int,
        val gender: String,
        val id: Int,
        val profile_url: String,
        val screen_name: String,
        val statuses_count: Int
    )

    override suspend fun start() {
        super.start()
        launch {
            run()
        }
    }

    suspend fun run() {
        val fetchInterval = 3 * 3600_000L
        WeiboCommentTable.fetchInterval = fetchInterval
        WeiboCommentTable.refreshInterval = 24 * 3600_000L
        while (this.isActive) {
            val commentIdList = CacheConnection.execute {
                WeiboCommentTable.loadTask(WeiboCommentTable.commentId) {
                    WeiboCommentTable.subCommentCount greater 0
                }.limit(1000).toList()
            }.map { it[WeiboCommentTable.commentId] }
            for (commentId in commentIdList) {
                val (subCommentList, userList) = fetch(commentId)
                CacheConnection.execute {
                    upsertSubcomment(subCommentList, commentId)
                    upsertUser(userList)
                    WeiboCommentTable.finished {
                        WeiboCommentTable.commentId eq commentId
                    }
                }
            }
            println("finished weibo comment")
            delay(fetchInterval)
            CacheConnection.execute {
                WeiboCommentTable.refresh {
                    WeiboCommentTable.subCommentCount greater 0
                }
            }
        }
    }

    // Sat Feb 29 22:12:45 +0800 2020
    private val commentDateFormat = DateTimeFormatter.ofPattern("EEE MMM dd HH:mm:SS XXXX yyyy", Locale.US)
    private suspend fun fetch(commentId: String): Pair<ArrayList<SubComment>, ArrayList<WeiboUser>> {
        val subCommentList = arrayListOf<SubComment>()
        val userList = arrayListOf<WeiboUser>()
        val urlPrefix = "https://m.weibo.cn/comments/hotFlowChild?cid=$commentId&max_id_type=0&max_id="
        var maxId = ""
        do {
            val url = "$urlPrefix$maxId"
            val resp = vertx.eventBus().requestAwait<JsonObject>(EventbusAddress.downloader,
                jsonObjectOf("url" to url)).body()
            val respText = resp.getString("body")
            println("got weibo subComment of comment $commentId")
            val respJson = JSON.parseObject(respText, RespJson::class.java)
            maxId = respJson.max_id.toString()
            for (subComment in respJson.data) {
                subComment.timestamp = ZonedDateTime.from(commentDateFormat.parse(subComment.created_at)).toEpochSecond()
                subComment.parsedContent = Jsoup.parse(subComment.text).text()
                subCommentList.add(subComment)
                val user = subComment.user
                userList.add(WeiboUser(user.id.toString(), user.screen_name))
            }
        } while (respJson.max_id != 0L)
        return Pair(subCommentList, userList)
    }

    private fun Transaction.upsertSubcomment(commentList: List<SubComment>, commentId: String) {
        connection.prepareStatement(
            "insert into ${WeiboSubCommentTable.tableName}(" +
                "${WeiboSubCommentTable.commentId.name}, " +
                "${WeiboSubCommentTable.subCommentId.name}, " +
                "${WeiboSubCommentTable.authorId.name}, " +
                "${WeiboSubCommentTable.createTime.name}, " +
                "${WeiboSubCommentTable.content.name}, " +
                "${WeiboSubCommentTable.voteupCount.name}) " +
                "values(?,?,?,?,?,?) " +
                "on conflict(${WeiboSubCommentTable.subCommentId.name}) " +
                "do update set " +
                "${WeiboSubCommentTable.voteupCount.name}=excluded.${WeiboCommentTable.voteupCount.name};")
            .apply {
                for (comment in commentList) {
                    setString(1, commentId)
                    setString(2, comment.id)
                    setString(3, comment.user.id.toString())
                    setLong(4, comment.timestamp)
                    setString(5, comment.parsedContent)
                    setInt(6, comment.like_count)
                    addBatch()
                }
                executeBatch()
            }
    }

}