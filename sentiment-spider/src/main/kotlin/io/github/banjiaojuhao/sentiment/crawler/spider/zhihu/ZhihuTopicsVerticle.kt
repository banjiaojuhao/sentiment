package io.github.banjiaojuhao.sentiment.crawler.spider.zhihu

import io.github.banjiaojuhao.sentiment.crawler.config.EventbusAddress
import io.github.banjiaojuhao.sentiment.crawler.config.MyConfig
import io.github.banjiaojuhao.sentiment.crawler.config.getInterval
import io.github.banjiaojuhao.sentiment.crawler.config.zhihu
import io.github.banjiaojuhao.sentiment.crawler.persistence.internal.CacheConnection
import io.github.banjiaojuhao.sentiment.crawler.persistence.internal.ZhihuQuestionTable
import com.alibaba.fastjson.JSON
import io.vertx.core.json.JsonObject
import io.vertx.kotlin.core.eventbus.requestAwait
import io.vertx.kotlin.core.json.jsonObjectOf
import io.vertx.kotlin.coroutines.CoroutineVerticle
import kotlinx.coroutines.*
import org.jetbrains.exposed.sql.Transaction


class ZhihuTopicsVerticle : CoroutineVerticle() {
    private data class Paging(
        val is_end: Boolean,
        val is_start: Boolean,
        val next: String,
        val previous: String
    )

    private data class Author(
        val avatar_url: String,
        val avatar_url_template: String,
        val gender: Int,
        val headline: String,
        val id: String,
        val is_advertiser: Boolean,
        val is_org: Boolean,
        val name: String,
        val type: String,
        val url: String,
        val url_token: String,
        val user_type: String
    )

    private data class Question(
        val id: Int,
        val author: Author,
        val created: Long,
        val title: String,
        val comment_count: Int,
        val answer_count: Int,
        val follower_count: Int,
        val question_type: String,
        val url: String,
        val type: String,
        val is_following: Boolean
    )

    private data class Data(
        val attached_info: String,
        val target: Question,
        val type: String
    )

    private data class TopicQuestionResp(
        val `data`: List<Data>,
        val paging: Paging
    )

    enum class Topics(val topicsId: String) {
        HITSZ("20093675")
    }

    private val questionFetchInterval = MyConfig.config.getInterval(zhihu.topic.run_interval)

    override suspend fun start() {
        super.start()
        launch {
            run(Topics.HITSZ.topicsId)
        }
    }

    /**
     * get all related question id and store into database
     * for further crawling
     * just save question_id
     */
    suspend fun run(topicsId: String) {

        while (this.isActive) {
            do {
                val (questionList, userList) = fetch(topicsId) ?: break
                CacheConnection.execute {
                    upsertQuestion(questionList)
                    upsertUser(userList)
                }
                println("finished zhihu topics")
            } while (false)
            delay(questionFetchInterval)
        }
    }

    private suspend fun fetch(topicsId: String): Pair<ArrayList<Question>, ArrayList<ZhihuUser>>? {
        var topicsUrl = "https://www.zhihu.com/api/v4/topics/$topicsId/feeds/timeline_question?include=data%5B%3F(target.type%3Dtopic_sticky_module)%5D.target.data%5B%3F(target.type%3Danswer)%5D.target.content%2Crelationship.is_authorized%2Cis_author%2Cvoting%2Cis_thanked%2Cis_nothelp%3Bdata%5B%3F(target.type%3Dtopic_sticky_module)%5D.target.data%5B%3F(target.type%3Danswer)%5D.target.is_normal%2Ccomment_count%2Cvoteup_count%2Ccontent%2Crelevant_info%2Cexcerpt.author.badge%5B%3F(type%3Dbest_answerer)%5D.topics%3Bdata%5B%3F(target.type%3Dtopic_sticky_module)%5D.target.data%5B%3F(target.type%3Darticle)%5D.target.content%2Cvoteup_count%2Ccomment_count%2Cvoting%2Cauthor.badge%5B%3F(type%3Dbest_answerer)%5D.topics%3Bdata%5B%3F(target.type%3Dtopic_sticky_module)%5D.target.data%5B%3F(target.type%3Dpeople)%5D.target.answer_count%2Carticles_count%2Cgender%2Cfollower_count%2Cis_followed%2Cis_following%2Cbadge%5B%3F(type%3Dbest_answerer)%5D.topics%3Bdata%5B%3F(target.type%3Danswer)%5D.target.annotation_detail%2Ccontent%2Chermes_label%2Cis_labeled%2Crelationship.is_authorized%2Cis_author%2Cvoting%2Cis_thanked%2Cis_nothelp%3Bdata%5B%3F(target.type%3Danswer)%5D.target.author.badge%5B%3F(type%3Dbest_answerer)%5D.topics%3Bdata%5B%3F(target.type%3Darticle)%5D.target.annotation_detail%2Ccontent%2Chermes_label%2Cis_labeled%2Cauthor.badge%5B%3F(type%3Dbest_answerer)%5D.topics%3Bdata%5B%3F(target.type%3Dquestion)%5D.target.annotation_detail%2Ccomment_count%3B"
        val questionList = arrayListOf<Question>()
        val userList = arrayListOf<ZhihuUser>()
        do {
            val resp = vertx.eventBus().requestAwait<JsonObject>(EventbusAddress.downloader,
                jsonObjectOf("url" to topicsUrl)).body()
            val respText = resp.getString("body")
            println("got topics $topicsId offset ${topicsUrl.substringAfter("offset=", "0").substringBefore("&")}")
            val topicQuestionResp = try {
                JSON.parseObject(respText, TopicQuestionResp::class.java)
            } catch (e: Exception) {
                println("failed to parse json on $topicsUrl")
                println(respText)
                e.printStackTrace()
                return null
            }
            topicsUrl = topicQuestionResp.paging.next

            for (item in topicQuestionResp.data) {
                questionList.add(item.target)
                val user = item.target.author
                userList.add(ZhihuUser(user.id, user.url_token, user.name))
            }
        } while (this.isActive && topicQuestionResp.data.size == 10)
        return Pair(questionList, userList)
    }

    private fun Transaction.upsertQuestion(questionList: List<Question>) {
        val upsertQuestion = this.connection.prepareStatement(
            "insert into ${ZhihuQuestionTable.tableName}" +
                "(${ZhihuQuestionTable.questionId.name}, " +
                "${ZhihuQuestionTable.authorId.name}, " +
                "${ZhihuQuestionTable.createTime.name}, " +
                "${ZhihuQuestionTable.commentCount.name}, " +
                "${ZhihuQuestionTable.followerCount.name}, " +
                "${ZhihuQuestionTable.answerCount.name}, " +
                "${ZhihuQuestionTable.title.name}) " +
                "values(?,?,?,?,?,?,?) " +
                "on conflict(${ZhihuQuestionTable.questionId.name}) " +
                "do update set " +
                "${ZhihuQuestionTable.title.name}=excluded.${ZhihuQuestionTable.title.name}, " +
                "${ZhihuQuestionTable.commentCount.name}=excluded.${ZhihuQuestionTable.commentCount.name}, " +
                "${ZhihuQuestionTable.answerCount.name}=excluded.${ZhihuQuestionTable.answerCount.name}, " +
                "${ZhihuQuestionTable.followerCount.name}=excluded.${ZhihuQuestionTable.followerCount.name};")
        for (question in questionList) {
            upsertQuestion.setString(1, question.id.toString())
            upsertQuestion.setString(2, question.author.id)
            upsertQuestion.setLong(3, question.created)
            upsertQuestion.setInt(4, question.comment_count)
            upsertQuestion.setInt(5, question.follower_count)
            upsertQuestion.setInt(6, question.answer_count)
            upsertQuestion.setString(7, question.title)
            upsertQuestion.addBatch()
        }
        upsertQuestion.executeBatch()
    }
}




