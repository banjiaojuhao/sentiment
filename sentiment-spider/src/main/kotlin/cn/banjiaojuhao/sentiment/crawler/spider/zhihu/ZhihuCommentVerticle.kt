package cn.banjiaojuhao.sentiment.crawler.spider.zhihu

import cn.banjiaojuhao.sentiment.crawler.config.EventbusAddress
import cn.banjiaojuhao.sentiment.crawler.config.MyConfig
import cn.banjiaojuhao.sentiment.crawler.config.getInterval
import cn.banjiaojuhao.sentiment.crawler.config.zhihu
import cn.banjiaojuhao.sentiment.crawler.persistence.internal.CacheConnection
import cn.banjiaojuhao.sentiment.crawler.persistence.internal.ZhihuAnswerTable
import cn.banjiaojuhao.sentiment.crawler.persistence.internal.ZhihuCommentTable
import com.alibaba.fastjson.JSON
import io.vertx.core.json.JsonObject
import io.vertx.kotlin.core.eventbus.requestAwait
import io.vertx.kotlin.core.json.jsonObjectOf
import io.vertx.kotlin.coroutines.CoroutineVerticle
import kotlinx.coroutines.*
import org.jetbrains.exposed.sql.Transaction

class ZhihuCommentVerticle : CoroutineVerticle() {

    private data class RespJson(
        val collapsed_counts: Int,
        val common_counts: Int,
        val `data`: List<Comment>,
        val featured_counts: Int,
        val paging: Paging,
        val reviewing_counts: Int
    )

    private data class Comment(
        val allow_delete: Boolean,
        val allow_like: Boolean,
        val allow_reply: Boolean,
        val allow_vote: Boolean,
        val author: Author,
        val can_collapse: Boolean,
        val can_recommend: Boolean,
        val collapsed: Boolean,
        val content: String,
        val created_time: Long,
        val disliked: Boolean,
        val featured: Boolean,
        val id: Int,
        val is_author: Boolean,
        val is_delete: Boolean,
        val is_parent_author: Boolean,
        val reply_to_author: Author?,
        val resource_type: String,
        val reviewing: Boolean,
        val type: String,
        val url: String,
        val vote_count: Int,
        val voting: Boolean
    )

    private data class Author(
        val member: Member,
        val role: String
    )

    private data class Member(
        val avatar_url: String,
        val avatar_url_template: String,
        val gender: Int,
        val headline: String,
        val id: String,
        val name: String,
        val url: String,
        val url_token: String,
        val user_type: String
    )

    private data class Paging(
        val is_end: Boolean,
        val is_start: Boolean,
        val next: String,
        val previous: String,
        val totals: Int
    )

    private data class CommentTask(val questionId: String, val isToAnswer: Boolean, val parentId: String, val latestCommentTime: Long)

    private val fetchInterval = MyConfig.config.getInterval(zhihu.comment.run_interval)
    private val refreshInterval = MyConfig.config.getInterval(zhihu.comment.refresh_interval)

    override suspend fun start() {
        super.start()
        launch {
            run()
        }
    }

    suspend fun run() {
        ZhihuAnswerTable.fetchInterval = fetchInterval
        ZhihuAnswerTable.refreshInterval = refreshInterval
        CacheConnection.execute {
            ZhihuAnswerTable.reset()
        }
        while (this.isActive) {
            val commentTaskList = loadTask()
            for (commentTask in commentTaskList) {
                val (commList, userList) = fetch(commentTask) ?: continue
                CacheConnection.execute {
                    upsertComment(commList, commentTask)
                    upsertUser(userList)
                    if (commentTask.isToAnswer) {
                        ZhihuAnswerTable.finished {
                            ZhihuAnswerTable.answerId eq commentTask.parentId
                        }
                    }
                }
                println("finished zhihu ${if (commentTask.isToAnswer) "answer" else "question"} comment ${commentTask.parentId}")
            }
            println("finished all zhihu comment")
            delay(fetchInterval)
            CacheConnection.execute {
                ZhihuAnswerTable.refresh {
                    ZhihuAnswerTable.commentCount greater 0
                }
            }
        }
    }

    private suspend fun loadTask(): List<CommentTask> {
        val result = arrayListOf<CommentTask>()
        CacheConnection.execute {
            ZhihuAnswerTable.loadTask(ZhihuAnswerTable.questionId,
                ZhihuAnswerTable.answerId,
                ZhihuAnswerTable.latestCommentTime) {
                ZhihuAnswerTable.commentCount greater 0
            }.limit(1000).toList()
        }.mapTo(result) {
            CommentTask(it[ZhihuAnswerTable.questionId], true, it[ZhihuAnswerTable.answerId], it[ZhihuAnswerTable.latestCommentTime])
        }
        return result
    }

    private suspend fun fetch(task: CommentTask): Pair<ArrayList<Comment>, Collection<ZhihuUser>>? {
        val commentList = arrayListOf<Comment>()
        val userList = hashMapOf<String, ZhihuUser>()
        var url = "https://www.zhihu.com/api/v4/${if (task.isToAnswer) "answers" else "questions"}/${task.parentId}/comments?order=reverse&limit=20&status=open"
        stopFetch@
        do {
            val resp = vertx.eventBus().requestAwait<JsonObject>(EventbusAddress.downloader,
                jsonObjectOf("url" to url)).body()
            val respText = resp.getString("body")
            println("got zhihu ${task.parentId} comment offset ${url.substringAfter("offset=", "0").substringBefore("&")}")
            val respJson = try {
                JSON.parseObject(respText, RespJson::class.java)
            } catch (e: Exception) {
                if ("NotFoundError" in respText) {
                    println("failed to parse json on $url due to NotFoundError")
                    return null
                } else {
                    println("failed to parse json on $url")
                    println(respText)
                    e.printStackTrace()
                    return null
                }
            }
            url = respJson.paging.next
            for (comment in respJson.data) {
                if (comment.created_time <= task.latestCommentTime) {
                    break@stopFetch
                }
                commentList.add(comment)
                comment.author.member.apply {
                    userList[id] = ZhihuUser(id, url_token, name)
                }
                comment.reply_to_author?.member?.apply {
                    userList[id] = ZhihuUser(id, url_token, name)
                }
            }
            if (respJson.paging.is_end) {
                break
            }
        } while (this.isActive)
        return Pair(commentList, userList.values)
    }

    private fun Transaction.upsertComment(commentList: List<Comment>, task: CommentTask) {
        val upsertCommStmt = this.connection.prepareStatement(
            "insert into ${ZhihuCommentTable.tableName}(" +
                "${ZhihuCommentTable.questionId.name}, " +
                "${ZhihuCommentTable.commentId.name}, " +
                "${ZhihuCommentTable.parentId.name}, " +
                "${ZhihuCommentTable.authorId.name}, " +
                "${ZhihuCommentTable.content.name}, " +
                "${ZhihuCommentTable.createTime.name}, " +
                "${ZhihuCommentTable.voteCount.name}, " +
                "${ZhihuCommentTable.replyToAuthorId.name}, " +
                "${ZhihuCommentTable.isToAnswer.name}) " +
                "values(?,?,?,?,?,?,?,?,?) " +
                "on conflict(${ZhihuCommentTable.commentId.name}) " +
                "do update set " +
                "${ZhihuCommentTable.voteCount.name}=excluded.${ZhihuCommentTable.voteCount.name}, " +
                "${ZhihuCommentTable.syncTime.name}=0;")
        upsertCommStmt.apply {
            for (comment in commentList) {
                setString(1, task.questionId)
                setString(2, comment.id.toString())
                setString(3, task.parentId)
                setString(4, comment.author.member.id)
                setString(5, comment.content)
                setLong(6, comment.created_time)
                setInt(7, comment.vote_count)
                setString(8, comment.reply_to_author?.member?.id ?: "null")
                setBoolean(9, task.isToAnswer)
                addBatch()
            }
            executeBatch()
        }
    }
}