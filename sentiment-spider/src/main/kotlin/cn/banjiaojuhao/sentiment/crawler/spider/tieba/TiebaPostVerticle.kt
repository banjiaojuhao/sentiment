package cn.banjiaojuhao.sentiment.crawler.spider.tieba

import cn.banjiaojuhao.sentiment.crawler.config.EventbusAddress
import cn.banjiaojuhao.sentiment.crawler.config.MyConfig
import cn.banjiaojuhao.sentiment.crawler.config.getInterval
import cn.banjiaojuhao.sentiment.crawler.config.tieba
import cn.banjiaojuhao.sentiment.crawler.persistence.internal.*
import cn.banjiaojuhao.sentiment.crawler.persistence.updatePatch
import com.alibaba.fastjson.JSON
import io.vertx.core.json.JsonObject
import io.vertx.kotlin.core.eventbus.requestAwait
import io.vertx.kotlin.core.json.jsonObjectOf
import io.vertx.kotlin.coroutines.CoroutineVerticle
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import org.jetbrains.exposed.sql.Transaction
import org.jetbrains.exposed.sql.and
import org.jetbrains.exposed.sql.select
import org.jetbrains.exposed.sql.update
import org.jsoup.Jsoup
import org.jsoup.parser.Parser
import java.time.LocalDateTime
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter

class TiebaPostVerticle : CoroutineVerticle() {

    private val postFetchInterval = MyConfig.config.getInterval(tieba.post.run_interval)
    private val formatterYMDHM = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm")

    private data class Author(
        val portrait: String,
        val user_id: Long,
        val user_name: String?,
        val user_nickname: String?
    )

    private data class Content(
        val comment_num: Int,
        val post_id: Long,
        val post_index: Int,
        val post_no: Int,
        var date: String? = null
    )

    private data class PostInfo(
        val author: Author,
        val content: Content
    )

    private data class PostItem(val content: String, val postInfo: PostInfo, val replyDate: Long)

    override suspend fun start() {
        super.start()
        launch {
            run()
        }
    }

    private suspend fun loadTask(): List<String> {
        val updateTag = System.currentTimeMillis()
        return CacheConnection.execute {
            TiebaThreadTable.updatePatch({
                (TiebaThreadTable.replyNum greater 0) and
                    (TiebaThreadTable.updated eq false) and
                    (TiebaThreadTable.id inSubQuery filterPassThreadId) and
                    (TiebaThreadTable.triedUpdateTime less updateTag - postFetchInterval)
            }, 1000) {
                it[triedUpdateTime] = updateTag
            }

            TiebaThreadTable.slice(TiebaThreadTable.threadId)
                .select {
                    (TiebaThreadTable.updated eq false) and
                        (TiebaThreadTable.triedUpdateTime eq updateTag)
                }.toList()
        }.map { it[TiebaThreadTable.threadId] }
    }

    private suspend fun fetchPost(threadId: String) {
        val urlPrefix = "http://tieba.baidu.com/p/$threadId?pn="
        val visitedPostSet = hashSetOf<Long>()
        for (pageNumber in 1..Int.MAX_VALUE) {
            val url = "$urlPrefix$pageNumber"
            val resp = vertx.eventBus().requestAwait<JsonObject>(EventbusAddress.downloader,
                jsonObjectOf("url" to url)).body()
            val respText = resp.getString("body")
            println("got $url")
            val doc = Jsoup.parse(respText, url, Parser.htmlParser())


            // extract data
            val userList = hashMapOf<String, TiebaUser>()
            val postList = doc.select("#j_p_postlist > div:not([ad-dom-img])")
                .mapNotNull { post ->
                    val content = post.select("div.d_post_content").text()

                    val data_field = post.attr("data-field")
                    val postInfo = try {
                        JSON.parseObject(data_field, PostInfo::class.java)
                    } catch (e: Exception) {
                        println("failed to parse json on $url")
                        println(data_field)
                        e.printStackTrace()
                        return
                    }
                    // In some posts, date is placed in html.
                    if (postInfo.content.date == null) {
                        postInfo.content.date = post.select("div.d_post_content_main > div.core_reply > " +
                            "div.core_reply_tail > div.post-tail-wrap > span").last().text()
                    }
                    val timestamp = LocalDateTime.parse(postInfo.content.date, formatterYMDHM)
                        .toEpochSecond(ZoneOffset.ofHours(8))
                    val userId = postInfo.author.portrait
                    val nickname = postInfo.author.user_nickname ?: postInfo.author.user_name ?: ""
                    userList[userId] = TiebaUser(userId, nickname)
                    if (!visitedPostSet.contains(postInfo.content.post_id)) {
                        visitedPostSet.add(postInfo.content.post_id)
                        PostItem(content, postInfo, timestamp)
                    } else {
                        null
                    }
                }
            if (postList.isEmpty()) {
                println("end post spider of thread $threadId")
                break
            }

            /*
            save data into db
             */
            CacheConnection.execute {
                upsertUsers(userList.values)
                upsertPost(threadId, postList)
            }
        }
        CacheConnection.execute {
            TiebaThreadTable.update({
                TiebaThreadTable.threadId eq threadId
            }) {
                it[updated] = true
            }
        }
    }

    private fun Transaction.upsertPost(threadId: String, postList: List<PostItem>) {
        connection.prepareStatement(
            "insert into ${TiebaPostTable.tableName}(" +
                "${TiebaPostTable.threadId.name}, " +
                "${TiebaPostTable.postId.name}, " +
                "${TiebaPostTable.authorId.name}, " +
                "${TiebaPostTable.commentNum.name}, " +
                "${TiebaPostTable.content.name}, " +
                "${TiebaPostTable.postNo.name}, " +
                "${TiebaPostTable.replyDate.name}) " +
                "values(?,?,?,?,?,?,?) " +
                "on conflict(${TiebaPostTable.postId.name}) " +
                "do update set " +
                "${TiebaPostTable.commentNum.name}=excluded.${TiebaPostTable.commentNum.name}, " +
                "${TiebaPostTable.syncTime.name}=0;")
            .apply {
                for (post in postList) {
                    val postInfo = post.postInfo
                    val postId = postInfo.content.post_id.toString()

                    setString(1, threadId)
                    setString(2, postId)
                    setString(3, postInfo.author.portrait)
                    setInt(4, postInfo.content.comment_num)
                    setString(5, post.content)
                    setInt(6, postInfo.content.post_no)
                    setLong(7, post.replyDate)
                    addBatch()
                }
                executeBatch()
            }
    }

    suspend fun run() {
        CacheConnection.execute {
            TiebaThreadTable.update({
                TiebaThreadTable.updated eq false
            }) {
                it[triedUpdateTime] = 0
            }
        }
        while (true) {
            val postIdList = loadTask()
            for (postId in postIdList) {
                println("fetching post $postId")
                try {
                    fetchPost(postId)
                } catch (e: Exception) {
                    println("error when fetch tieba post")
                    e.printStackTrace()
                }
            }
            delay(postFetchInterval)
        }
    }
}