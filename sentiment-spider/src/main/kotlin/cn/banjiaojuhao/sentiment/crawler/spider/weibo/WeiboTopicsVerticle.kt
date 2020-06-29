package cn.banjiaojuhao.sentiment.crawler.spider.weibo

import cn.banjiaojuhao.sentiment.crawler.config.EventbusAddress
import cn.banjiaojuhao.sentiment.crawler.persistence.internal.CacheConnection
import cn.banjiaojuhao.sentiment.crawler.persistence.internal.WeiboPostTable
import com.alibaba.fastjson.annotation.JSONField
import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import io.vertx.core.json.Json
import io.vertx.core.json.JsonObject
import io.vertx.kotlin.core.eventbus.requestAwait
import io.vertx.kotlin.core.json.jsonObjectOf
import io.vertx.kotlin.coroutines.CoroutineVerticle
import kotlinx.coroutines.*
import org.jetbrains.exposed.sql.Transaction
import org.jsoup.Jsoup
import java.net.URLEncoder
import java.time.LocalDateTime
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter

class WeiboTopicsVerticle : CoroutineVerticle() {

    @JsonIgnoreProperties(ignoreUnknown = true)
    private data class RespJson(
        val `data`: Data,
        val ok: Int
    )

    @JsonIgnoreProperties(ignoreUnknown = true)
    private data class Data(
        val cards: List<Card>
    )

    @JsonIgnoreProperties(ignoreUnknown = true)
    private data class Card(
        val card_type: Int,
        val card_type_name: String,
        val mblog: Mblog
    )

    @JsonIgnoreProperties(ignoreUnknown = true)
    private data class Mblog(
        val text: String,
        @JSONField(deserialize = false)
        var parsedContent: String = "",
        val attitudes_count: Int,
        val comments_count: Int,
        val reposts_count: Int,
        val created_at: String,
        @JSONField(deserialize = false)
        var createTime: Long = 0,
        val id: String,
        val idstr: String,
        val mid: String,
        val isLongText: Boolean,
        val user: User
    )

    @JsonIgnoreProperties(ignoreUnknown = true)
    private data class User(
        val description: String,
        val follow_count: Int,
        val followers_count: Int,
        val gender: String,
        val id: Long,
        val screen_name: String,
        val statuses_count: Int
    )

    override suspend fun start() {
        super.start()
        launch {
            run("")
        }
    }

    suspend fun run(keyword: String) {
        val updateInterval = 24 * 3600_000L
        while (this.isActive) {
            val (postList, userList) = fetch(keyword)
            CacheConnection.execute {
                upsertPost(postList)
                upsertUser(userList)
            }
            println("finished weibo topics")
            delay(updateInterval)
        }
    }

    private suspend fun fetch(keyword: String): Pair<ArrayList<Mblog>, ArrayList<WeiboUser>> {
        val postList = arrayListOf<Mblog>()
        val userList = arrayListOf<WeiboUser>()
        val urlPrefix = "https://m.weibo.cn/api/container/getIndex?containerid=100103%26type%3D61%26q%3D" +
            "${URLEncoder.encode(keyword, Charsets.UTF_8)}%26t%3D0&page_type=searchall&page="
        for (pageNumber in 1..Int.MAX_VALUE) {
            val url = "$urlPrefix$pageNumber"
            val resp = vertx.eventBus().requestAwait<JsonObject>(EventbusAddress.downloader,
                jsonObjectOf("url" to url)).body()
            val respText = resp.getString("body")
            val respTime = resp.getJsonObject("headers").getString("Date")
            val respDate = LocalDateTime.parse(respTime, DateTimeFormatter.RFC_1123_DATE_TIME)

            val respJson = Json.decodeValue(respText, RespJson::class.java)
            println("got weibo topics $pageNumber")
            delay(1000)
//            val respText = resp.bodyAsString()
//            val respJson = JSON.parseObject(respText, RespJson::class.java)
            if (respJson.ok != 1) {
                break
            }
            for (card in respJson.data.cards) {
                val date = card.mblog.created_at.split("-")
                card.mblog.createTime = when (date.size) {
                    2 -> {
                        respDate.withMonth(date[0].toInt())
                            .withDayOfMonth(date[1].toInt())
                            .toEpochSecond(ZoneOffset.ofHours(8))
                    }
                    3 -> {
                        respDate.withYear(date[0].toInt())
                            .withMonth(date[1].toInt())
                            .withDayOfMonth(date[2].toInt())
                            .toEpochSecond(ZoneOffset.ofHours(8))
                    }
                    else -> {
                        0L
                    }
                }
                card.mblog.parsedContent = Jsoup.parse(card.mblog.text).text()
                postList.add(card.mblog)
                val user = card.mblog.user
                userList.add(WeiboUser(user.id.toString(), user.screen_name))
            }
        }
        return Pair(postList, userList)
    }

    private fun Transaction.upsertPost(postList: List<Mblog>) {
        connection.prepareStatement(
            "insert into ${WeiboPostTable.tableName}(" +
                "${WeiboPostTable.postId.name}, " +
                "${WeiboPostTable.authorId.name}, " +
                "${WeiboPostTable.createTime.name}, " +
                "${WeiboPostTable.content.name}, " +
                "${WeiboPostTable.needUpdateContent.name}, " +
                "${WeiboPostTable.repostCount.name}, " +
                "${WeiboPostTable.voteupCount.name}, " +
                "${WeiboPostTable.commentCount.name}) " +
                "values(?,?,?,?,?,?,?,?) " +
                "on conflict(${WeiboPostTable.postId}) " +
                "do update set " +
                "${WeiboPostTable.repostCount.name}=excluded.${WeiboPostTable.repostCount.name}, " +
                "${WeiboPostTable.voteupCount.name}=excluded.${WeiboPostTable.voteupCount.name}, " +
                "${WeiboPostTable.commentCount.name}=excluded.${WeiboPostTable.commentCount.name};")
            .apply {
                for (post in postList) {
                    setString(1, post.id)
                    setString(2, post.user.id.toString())
                    setLong(3, post.createTime)
                    setString(4, post.parsedContent)
                    setBoolean(5, post.isLongText)
                    setInt(6, post.reposts_count)
                    setInt(7, post.attitudes_count)
                    setInt(8, post.comments_count)
                    addBatch()
                }
                executeBatch()
            }
    }
}