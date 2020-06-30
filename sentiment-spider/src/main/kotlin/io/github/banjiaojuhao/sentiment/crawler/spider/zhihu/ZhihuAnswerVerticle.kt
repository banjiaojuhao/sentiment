package io.github.banjiaojuhao.sentiment.crawler.spider.zhihu

import io.github.banjiaojuhao.sentiment.crawler.config.EventbusAddress
import io.github.banjiaojuhao.sentiment.crawler.config.MyConfig
import io.github.banjiaojuhao.sentiment.crawler.config.getInterval
import io.github.banjiaojuhao.sentiment.crawler.config.zhihu
import io.github.banjiaojuhao.sentiment.crawler.persistence.internal.CacheConnection
import io.github.banjiaojuhao.sentiment.crawler.persistence.internal.ZhihuAnswerTable
import io.github.banjiaojuhao.sentiment.crawler.persistence.internal.ZhihuQuestionTable
import com.alibaba.fastjson.JSON
import io.vertx.core.json.JsonObject
import io.vertx.kotlin.core.eventbus.requestAwait
import io.vertx.kotlin.core.json.jsonObjectOf
import io.vertx.kotlin.coroutines.CoroutineVerticle
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import org.jetbrains.exposed.sql.Transaction
import org.jsoup.Jsoup


class ZhihuAnswerVerticle : CoroutineVerticle() {
    private data class ZhihuAnswerResp(
        val `data`: List<Answer>,
        val paging: Paging
    )

    private data class Answer(
        val author: Author,
        val comment_count: Int,
        val content: String,
        val created_time: Long,
        var excerpt: String,
        val id: Int,
        val updated_time: Long,
        val url: String,
        val voteup_count: Int
    )

    private data class Author(
        val follower_count: Int,
        val gender: Int,
        val id: String,
        val name: String,
        val url: String,
        val url_token: String
    )

    private data class Paging(
        val is_end: Boolean,
        val is_start: Boolean,
        val next: String,
        val previous: String,
        val totals: Int
    )

    override suspend fun start() {
        super.start()
        launch {
            run()
        }
    }

    private val fetchInterval = MyConfig.config.getInterval(zhihu.answer.run_interval)
    private val refreshInterval = MyConfig.config.getInterval(zhihu.answer.refresh_interval)

    suspend fun run() {
        ZhihuQuestionTable.fetchInterval = fetchInterval
        ZhihuQuestionTable.refreshInterval = refreshInterval

        CacheConnection.execute {
            ZhihuQuestionTable.reset {
                ZhihuQuestionTable.answerCount greater 0
            }
        }
        while (this.isActive) {
            val questionIdList = loadTask()
            for (questionId in questionIdList) {
                val (answerList, userList) = fetch(questionId) ?: continue
                CacheConnection.execute {
                    upsertAnswer(answerList, questionId)
                    upsertUser(userList)
                    ZhihuQuestionTable.finished {
                        ZhihuQuestionTable.questionId eq questionId
                    }
                }
                println("finished question $questionId")
            }
            println("finished all zhihu question")

            delay(fetchInterval)
            CacheConnection.execute {
                ZhihuQuestionTable.refresh {
                    ZhihuQuestionTable.answerCount greater 0
                }
            }
        }
    }

    private suspend fun loadTask(): List<String> {
        return CacheConnection.execute {
            ZhihuQuestionTable.loadTask(ZhihuQuestionTable.questionId) {
                ZhihuQuestionTable.answerCount greater 0
            }.limit(1000).toList()
        }.map { it[ZhihuQuestionTable.questionId] }
    }

    private suspend fun fetch(questionId: String): Pair<ArrayList<Answer>, ArrayList<ZhihuUser>>? {
        val answerList = arrayListOf<Answer>()
        val userList = arrayListOf<ZhihuUser>()
        var url = "https://www.zhihu.com/api/v4/questions/$questionId/answers?include=data%5B*%5D.is_normal%2Cadmin_closed_comment%2Creward_info%2Cis_collapsed%2Cannotation_action%2Cannotation_detail%2Ccollapse_reason%2Cis_sticky%2Ccollapsed_by%2Csuggest_edit%2Ccomment_count%2Ccan_comment%2Ccontent%2Ceditable_content%2Cvoteup_count%2Creshipment_settings%2Ccomment_permission%2Ccreated_time%2Cupdated_time%2Creview_info%2Crelevant_info%2Cquestion%2Cexcerpt%2Crelationship.is_authorized%2Cis_author%2Cvoting%2Cis_thanked%2Cis_nothelp%2Cis_labeled%2Cis_recognized%2Cpaid_info%2Cpaid_info_content%3Bdata%5B*%5D.mark_infos%5B*%5D.url%3Bdata%5B*%5D.author.follower_count%2Cbadge%5B*%5D.topics&limit=20&platform=mobile&sort_by=default"
        do {
            val resp = vertx.eventBus().requestAwait<JsonObject>(EventbusAddress.downloader,
                jsonObjectOf("url" to url)).body()
            val respText = resp.getString("body")
            println("got question $questionId offset ${url.substringAfter("offset", "0").substringBefore("&")}")
            val zhihuAnswerResp = try {
                JSON.parseObject(respText, ZhihuAnswerResp::class.java)
            } catch (e: Exception) {
                if (respText.contains("AuthenticationError")) {
                    // todo mark questions like this and fetch after login
                    println("failed to parse json on $url due to AuthenticationError")
                } else if (respText.contains("NotFoundError")) {
                    // todo mark questions like this and fetch after login
                    println("failed to parse json on $url due to NotFoundError")
                } else {
                    println("failed to parse json on $url")
                    println(respText)
                    e.printStackTrace()
                }
                return null
            }
            url = zhihuAnswerResp.paging.next
            for (answer in zhihuAnswerResp.data) {
                answer.excerpt = Jsoup.parse(answer.content).text()
                answerList.add(answer)
                val author = answer.author
                userList.add(ZhihuUser(author.id, author.url_token, author.name))
            }
        } while (this.isActive && !zhihuAnswerResp.paging.is_end)
        return Pair(answerList, userList)
    }

    private fun Transaction.upsertAnswer(answerList: List<Answer>, questionId: String) {
        connection.prepareStatement(
            "insert into ${ZhihuAnswerTable.tableName}(" +
                "${ZhihuAnswerTable.questionId.name}, " +
                "${ZhihuAnswerTable.answerId.name}, " +
                "${ZhihuAnswerTable.authorId.name}, " +
                "${ZhihuAnswerTable.createTime.name}, " +
                "${ZhihuAnswerTable.updateTime.name}, " +
                "${ZhihuAnswerTable.commentCount.name}, " +
                "${ZhihuAnswerTable.voteupCount.name}, " +
                "${ZhihuAnswerTable.content.name}, " +
                "${ZhihuAnswerTable.excerpt.name}) " +
                "values(?,?,?,?,?,?,?,?,?) " +
                "on conflict(${ZhihuAnswerTable.answerId.name}) " +
                "do update set " +
                "${ZhihuAnswerTable.updateTime.name}=excluded.${ZhihuAnswerTable.updateTime.name}, " +
                "${ZhihuAnswerTable.commentCount.name}=excluded.${ZhihuAnswerTable.commentCount.name}, " +
                "${ZhihuAnswerTable.voteupCount.name}=excluded.${ZhihuAnswerTable.voteupCount.name}, " +
                "${ZhihuAnswerTable.content.name}=excluded.${ZhihuAnswerTable.content.name}, " +
                "${ZhihuAnswerTable.syncTime.name}=0, " +
                "${ZhihuAnswerTable.excerpt.name}=excluded.${ZhihuAnswerTable.excerpt.name};")
            .apply {
                for (answer in answerList) {
                    setString(1, questionId)
                    setString(2, answer.id.toString())
                    setString(3, answer.author.id)
                    setLong(4, answer.created_time)
                    setLong(5, answer.updated_time)
                    setInt(6, answer.comment_count)
                    setInt(7, answer.voteup_count)
                    setString(8, answer.content)
                    setString(9, answer.excerpt)
                    addBatch()
                }
                executeBatch()
            }
    }
}

