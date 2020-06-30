package io.github.banjiaojuhao.sentiment.crawler.spider.weibo

import io.github.banjiaojuhao.sentiment.crawler.config.EventbusAddress
import io.github.banjiaojuhao.sentiment.crawler.persistence.internal.CacheConnection
import io.github.banjiaojuhao.sentiment.crawler.persistence.internal.WeiboCommentTable
import io.github.banjiaojuhao.sentiment.crawler.persistence.internal.WeiboPostTable
import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.annotation.JSONField
import io.vertx.core.json.JsonObject
import io.vertx.kotlin.core.eventbus.requestAwait
import io.vertx.kotlin.core.json.jsonObjectOf
import io.vertx.kotlin.coroutines.CoroutineVerticle
import kotlinx.coroutines.*
import org.jetbrains.exposed.sql.Transaction
import org.jsoup.Jsoup
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import java.util.*

class WeiboCommentVerticle : CoroutineVerticle() {
    private data class RespJson(
        val `data`: Data,
        val ok: Int
    )

    private data class Data(
        val `data`: List<Comment>,
        val max: Int,
        val max_id: Long,
        val max_id_type: Int,
        val total_number: Int
    )

    private data class Comment(
        val created_at: String,
        @JSONField(deserialize = false)
        var timestamp: Long = 0L,
        val floor_number: Int,
        val id: String,
        val like_count: Int,
        val max_id: Int,
        val mid: String,
        val rootid: String,
        val rootidstr: String,
        val source: String,
        val text: String,
        @JSONField(deserialize = false)
        var parsedContent: String = "",
        val total_number: Int,
        val user: User
    )

    private data class User(
        val avatar_hd: String,
        val close_blue_v: Boolean,
        val cover_image_phone: String,
        val description: String,
        val follow_count: Int,
        val follow_me: Boolean,
        val followers_count: Int,
        val following: Boolean,
        val gender: String,
        val id: Long,
        val like: Boolean,
        val like_me: Boolean,
        val mbrank: Int,
        val mbtype: Int,
        val profile_url: String,
        val screen_name: String,
        val statuses_count: Int,
        val urank: Int,
        val verified: Boolean,
        val verified_type: Int
    )

    override suspend fun start() {
        super.start()
        launch {
            run()
        }
    }

    suspend fun run() {
        val fetchInterval = 3 * 3600_000L
        WeiboPostTable.fetchInterval = fetchInterval
        WeiboPostTable.refreshInterval = 24 * 3600_000L
        while (this.isActive) {
            val postIdList = CacheConnection.execute {
                WeiboPostTable.loadTask(WeiboPostTable.postId) {
                    WeiboPostTable.commentCount greater 0
                }.limit(1000).toList()
            }.map { it[WeiboPostTable.postId] }
            for (postId in postIdList) {
                val (commentList, userList) = fetch(postId)
                CacheConnection.execute {
                    upsertComment(commentList, postId)
                    upsertUser(userList)
                    WeiboPostTable.finished {
                        WeiboPostTable.postId eq postId
                    }
                }
            }
            println("finished weibo post")
            delay(fetchInterval)
            CacheConnection.execute {
                WeiboPostTable.refresh {
                    WeiboPostTable.commentCount greater 0
                }
            }
        }
    }

    // Sat Feb 29 22:12:45 +0800 2020
    private val commentDateFormat = DateTimeFormatter.ofPattern("EEE MMM dd HH:mm:SS XXXX yyyy", Locale.US)
    private suspend fun fetch(postId: String): Pair<ArrayList<Comment>, ArrayList<WeiboUser>> {
        val commentList = arrayListOf<Comment>()
        val userList = arrayListOf<WeiboUser>()
        val urlPrefix = "https://m.weibo.cn/comments/hotflow?id=$postId&mid=$postId&max_id_type=0&max_id="
        var maxId = ""
        do {
            val url = "$urlPrefix$maxId"
            val resp = vertx.eventBus().requestAwait<JsonObject>(EventbusAddress.downloader,
                jsonObjectOf("url" to url)).body()
            val respText = resp.getString("body")
            println("got weibo comment of post $postId")
            val respJson = JSON.parseObject(respText, RespJson::class.java)
            maxId = respJson.data.max_id.toString()
            for (comment in respJson.data.data) {
                comment.timestamp = ZonedDateTime.from(commentDateFormat.parse(comment.created_at)).toEpochSecond()
                comment.parsedContent = Jsoup.parse(comment.text).text()
                commentList.add(comment)
                val user = comment.user
                userList.add(WeiboUser(user.id.toString(), user.screen_name))
            }
        } while (respJson.data.max_id != 0L)
        return Pair(commentList, userList)
    }

    private fun Transaction.upsertComment(commentList: List<Comment>, postId: String) {
        connection.prepareStatement(
            "insert into ${WeiboCommentTable.tableName}(" +
                "${WeiboCommentTable.postId.name}, " +
                "${WeiboCommentTable.commentId.name}, " +
                "${WeiboCommentTable.authorId.name}, " +
                "${WeiboCommentTable.createTime.name}, " +
                "${WeiboCommentTable.content.name}, " +
                "${WeiboCommentTable.voteupCount.name}, " +
                "${WeiboCommentTable.subCommentCount.name}) " +
                "values(?,?,?,?,?,?,?) " +
                "on conflict(${WeiboCommentTable.commentId.name}) " +
                "do update set " +
                "${WeiboCommentTable.voteupCount.name}=excluded.${WeiboCommentTable.voteupCount.name}, " +
                "${WeiboCommentTable.subCommentCount.name}=excluded.${WeiboCommentTable.subCommentCount.name};")
            .apply {
                for (comment in commentList) {
                    setString(1, postId)
                    setString(2, comment.id)
                    setString(3, comment.user.id.toString())
                    setLong(4, comment.timestamp)
                    setString(5, comment.parsedContent)
                    setInt(6, comment.like_count)
                    setInt(7, comment.total_number)
                    addBatch()
                }
                executeBatch()
            }
    }

}