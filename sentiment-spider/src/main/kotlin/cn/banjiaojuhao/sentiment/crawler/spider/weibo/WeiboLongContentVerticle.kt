package cn.banjiaojuhao.sentiment.crawler.spider.weibo

import cn.banjiaojuhao.sentiment.crawler.config.EventbusAddress
import cn.banjiaojuhao.sentiment.crawler.persistence.internal.CacheConnection
import cn.banjiaojuhao.sentiment.crawler.persistence.internal.WeiboPostTable
import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.annotation.JSONField
import io.vertx.core.json.JsonObject
import io.vertx.kotlin.core.eventbus.requestAwait
import io.vertx.kotlin.core.json.jsonObjectOf
import io.vertx.kotlin.coroutines.CoroutineVerticle
import kotlinx.coroutines.*
import org.jetbrains.exposed.sql.Transaction
import org.jetbrains.exposed.sql.select
import org.jsoup.Jsoup

class WeiboLongContentVerticle : CoroutineVerticle() {
    private data class RespJson(
        val `data`: Content,
        val ok: Int
    )

    private data class Content(
        val attitudes_count: Int,
        val comments_count: Int,
        var longTextContent: String,
        val ok: Int,
        val reposts_count: Int,
        @JSONField(deserialize = false)
        var postId: String = ""
    )

    override suspend fun start() {
        super.start()
        launch {
            run()
        }
    }

    suspend fun run() {
        val fetchInterval = 3 * 3600_000L
        while (this.isActive) {
            val postContentList = loadTask().map {
                fetch(it)
            }
            CacheConnection.execute {
                updateContent(postContentList)
            }
            println("finished weibo topics")
            delay(fetchInterval)
        }
    }

    private suspend fun loadTask() = CacheConnection.execute {
        WeiboPostTable.slice(WeiboPostTable.postId).select {
            WeiboPostTable.needUpdateContent eq true
        }.toList()
    }.map { it[WeiboPostTable.postId] }

    private suspend fun fetch(postId: String): Content {
        val url = "https://m.weibo.cn/statuses/extend?id=$postId"
        val resp = vertx.eventBus().requestAwait<JsonObject>(EventbusAddress.downloader,
            jsonObjectOf("url" to url)).body()
        val respText = resp.getString("body")
        println("got weibo long content of post $postId")
        val respJson = JSON.parseObject(respText, RespJson::class.java)
        val content = respJson.data
        content.longTextContent = Jsoup.parse(content.longTextContent).text()
        content.postId = postId
        return content
    }

    private fun Transaction.updateContent(postList: List<Content>) {
        connection.prepareStatement(
            "update ${WeiboPostTable.tableName} set " +
                "${WeiboPostTable.content.name}=?, " +
                "${WeiboPostTable.repostCount.name}=?, " +
                "${WeiboPostTable.voteupCount.name}=?, " +
                "${WeiboPostTable.commentCount.name}=?" +
                "${WeiboPostTable.needUpdateContent.name}=0" +
                "where ${WeiboPostTable.postId.name} == ?;")
            .apply {
                for (post in postList) {
                    setString(1, post.longTextContent)
                    setInt(2, post.reposts_count)
                    setInt(3, post.attitudes_count)
                    setInt(4, post.comments_count)
                    setString(5, post.postId)
                    addBatch()
                }
                executeBatch()
            }
    }

}