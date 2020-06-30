package io.github.banjiaojuhao.sentiment.crawler.spider.tieba

import io.github.banjiaojuhao.sentiment.crawler.config.EventbusAddress
import io.github.banjiaojuhao.sentiment.crawler.config.MyConfig
import io.github.banjiaojuhao.sentiment.crawler.config.getInterval
import io.github.banjiaojuhao.sentiment.crawler.config.tieba
import io.github.banjiaojuhao.sentiment.crawler.persistence.internal.CacheConnection
import io.github.banjiaojuhao.sentiment.crawler.persistence.internal.TiebaCommentTable
import io.github.banjiaojuhao.sentiment.crawler.persistence.internal.TiebaPostTable
import io.github.banjiaojuhao.sentiment.crawler.persistence.internal.filterPassThreadId
import io.github.banjiaojuhao.sentiment.crawler.persistence.updatePatch
import com.alibaba.fastjson.JSON
import io.vertx.core.json.JsonObject
import io.vertx.kotlin.core.eventbus.requestAwait
import io.vertx.kotlin.core.json.jsonObjectOf
import io.vertx.kotlin.coroutines.CoroutineVerticle
import kotlinx.coroutines.*
import org.jetbrains.exposed.sql.Transaction
import org.jetbrains.exposed.sql.and
import org.jetbrains.exposed.sql.select
import org.jetbrains.exposed.sql.update
import org.jsoup.Jsoup
import org.jsoup.parser.Parser
import java.time.LocalDateTime
import java.time.ZoneOffset
import java.util.regex.Pattern


class TiebaCommentVerticle : CoroutineVerticle() {
    private val commentFetchInterval = MyConfig.config.getInterval(tieba.comment.run_interval)
    private val commentRefreshInterval = MyConfig.config.getInterval(tieba.comment.refresh_interval)
    private val datePattern = Pattern.compile("""(\d+?)-(\d+?)-(\d+?) (\d+?):(\d+?)""")

    override suspend fun start() {
        super.start()
        launch {
            run()
        }
    }

    private data class CommentTask(val tid: String, val pid: String)

    private suspend fun loadTask(): List<CommentTask> {
        val updateTag = System.currentTimeMillis()

        return CacheConnection.execute {
            TiebaPostTable.updatePatch({
                (TiebaPostTable.commentNum greater 0) and
                    (TiebaPostTable.updated eq false) and
                    (TiebaPostTable.threadId inSubQuery filterPassThreadId) and
                    (TiebaPostTable.triedUpdateTime less updateTag - commentFetchInterval)
            }, 1000) {
                it[triedUpdateTime] = updateTag
            }

            TiebaPostTable.slice(TiebaPostTable.threadId, TiebaPostTable.postId).select {
                TiebaPostTable.triedUpdateTime eq updateTag
            }.toList()
        }.map { CommentTask(it[TiebaPostTable.threadId], it[TiebaPostTable.postId]) }
    }


    private data class CommentItem(val authorId: String, val replyToId: String,
                                   val content: String, val replyDate: Long)

    private suspend fun fetchComment(commentTask: CommentTask): Pair<HashMap<String, TiebaUser>, ArrayList<CommentItem>>? {
        val urlPrefix = "http://tieba.baidu.com/p/comment?tid=${commentTask.tid}&pid=${commentTask.pid}&pn="
        val userList = hashMapOf<String, TiebaUser>()
        val saveCommentList = arrayListOf<CommentItem>()
        for (pageNumber in 1..Int.MAX_VALUE) {
            val url = "$urlPrefix$pageNumber"
            val resp = vertx.eventBus().requestAwait<JsonObject>(EventbusAddress.downloader,
                jsonObjectOf("url" to url)).body()
            val respText = resp.getString("body")
            println("got $url")
            val doc = Jsoup.parse(respText, url, Parser.htmlParser())

            data class UserInfoJson(
                val id: String,
                val un: String?
            )

            val commentList = doc.select("li.lzl_single_post > div.lzl_cnt")
            if (commentList.isEmpty()) {
                println("end comment spider of thread:post ${commentTask.tid}:${commentTask.pid}")
                break
            }
            commentList.forEach { comment ->
                val authorInfoText = comment.select("a.j_user_card").attr("data-field")
                val authorInfo = try {
                    JSON.parseObject(authorInfoText, UserInfoJson::class.java)
                } catch (e: Exception) {
                    println("failed to parse json on $url")
                    println(authorInfoText)
                    e.printStackTrace()
                    return null
                }
                userList[authorInfo.id] = TiebaUser(authorInfo.id, authorInfo.un
                    ?: "")

                val replyToUser = comment.select("span.lzl_content_main > a.at")
                val replyToId = replyToUser.attr("portrait")
                val contentMain = comment.select("span.lzl_content_main").text()
                val content = if (replyToId != "") {
                    val replyToName = replyToUser.text()
                    userList[replyToId] = TiebaUser(replyToId, replyToName)
                    contentMain.substringAfter(":")
                } else {
                    contentMain
                }
                val replyDate = comment.select("div.lzl_content_reply > span.lzl_time").text()
                val matcher = datePattern.matcher(replyDate)
                if (!matcher.find()) {
                    println("err with date $replyDate")
                    return@forEach
                }
                val replyTimestamp = LocalDateTime.of(matcher.group(1).toInt(), matcher.group(2).toInt(),
                    matcher.group(3).toInt(), matcher.group(4).toInt(),
                    matcher.group(5).toInt())
                    .toEpochSecond(ZoneOffset.ofHours(8))
                saveCommentList.add(CommentItem(authorInfo.id, replyToId, content, replyTimestamp))
            }
        }
        return Pair(userList, saveCommentList)
    }

    private fun Transaction.saveComment(commentTask: CommentTask, commentList: List<CommentItem>) {
        connection.prepareStatement(
            "insert into ${TiebaCommentTable.tableName}(" +
                "${TiebaCommentTable.threadId.name}, " +
                "${TiebaCommentTable.postId.name}, " +
                "${TiebaCommentTable.authorId.name}, " +
                "${TiebaCommentTable.replyToId.name}, " +
                "${TiebaCommentTable.content.name}, " +
                "${TiebaCommentTable.replyDate.name}, " +
                "${TiebaCommentTable._uniqueId.name}) " +
                "values(?,?,?,?,?,?,?) " +
                "on conflict(${TiebaCommentTable._uniqueId.name}) " +
                "do nothing;")
            .apply {
                for (comment in commentList) {
                    setString(1, commentTask.tid)
                    setString(2, commentTask.pid)
                    setString(3, comment.authorId)
                    setString(4, comment.replyToId)
                    setString(5, comment.content)
                    setLong(6, comment.replyDate)
                    val tmpId = comment.authorId + comment.replyToId + comment.content
                    setString(7, "${commentTask.pid}${tmpId.length}${tmpId.hashCode()}")
                    addBatch()
                }
                executeBatch()
            }
    }

    suspend fun run() {
        CacheConnection.execute {
            TiebaPostTable.update({
                TiebaPostTable.updated eq false
            }) {
                it[this.triedUpdateTime] = 0
            }
        }
        while (this.isActive) {
            val commentTaskList = loadTask()
            for (commentTask in commentTaskList) {
                withContext(NonCancellable) {
                    val (newUserMap, commentList) = fetchComment(commentTask) ?: return@withContext
                    CacheConnection.execute {
                        upsertUsers(newUserMap.values)
                        saveComment(commentTask, commentList)
                        TiebaPostTable.update({
                            TiebaPostTable.postId eq commentTask.pid
                        }) {
                            it[this.updated] = true
                            it[this.triedUpdateTime] = System.currentTimeMillis()
                        }
                    }
                }
            }
            delay(commentFetchInterval)
            CacheConnection.execute {
                TiebaPostTable.update({
                    TiebaPostTable.triedUpdateTime less System.currentTimeMillis() - commentRefreshInterval
                }) {
                    it[this.updated] = false
                }
            }
        }
    }
}

