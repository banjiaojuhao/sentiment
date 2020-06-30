package io.github.banjiaojuhao.sentiment.crawler.spider.tieba

import io.github.banjiaojuhao.sentiment.crawler.config.EventbusAddress
import io.github.banjiaojuhao.sentiment.crawler.config.MyConfig
import io.github.banjiaojuhao.sentiment.crawler.config.getInterval
import io.github.banjiaojuhao.sentiment.crawler.config.tieba
import io.github.banjiaojuhao.sentiment.crawler.persistence.internal.CacheConnection
import io.github.banjiaojuhao.sentiment.crawler.persistence.internal.TiebaThreadTable
import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.annotation.JSONField
import io.vertx.core.json.JsonObject
import io.vertx.kotlin.core.eventbus.requestAwait
import io.vertx.kotlin.core.json.jsonObjectOf
import io.vertx.kotlin.coroutines.CoroutineVerticle
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import org.jetbrains.exposed.sql.Transaction
import org.jsoup.Jsoup
import org.jsoup.parser.Parser
import java.net.URLEncoder

class TiebaThreadVerticle : CoroutineVerticle() {
    private data class ThreadInfo(
        @JSONField(name = "author_name")
        val authorName: String?,
        @JSONField(name = "author_nickname")
        val authorNickname: String?,
        @JSONField(name = "author_portrait")
        val authorPortrait: String,
        @JSONField(name = "first_post_id")
        val firstPostId: Long,
        @JSONField(name = "frs_tpoint")
        val frsTpoint: Any?,
        @JSONField(name = "id")
        val id: Long,
        @JSONField(name = "is_bakan")
        val isBakan: Any?,
        @JSONField(name = "is_good")
        val isGood: Any?,
        @JSONField(name = "is_membertop")
        val isMembertop: Any?,
        @JSONField(name = "is_multi_forum")
        val isMultiForum: Any?,
        @JSONField(name = "is_protal")
        val isProtal: Any?,
        @JSONField(name = "is_top")
        val isTop: Any?,
        @JSONField(name = "reply_num")
        val replyNum: Int,
        @JSONField(name = "vid")
        val vid: String
    )

    private data class ThreadItem(val updateTag: String,
                                  val threadInfo: ThreadInfo,
                                  val title: String)

    private val threadUpdateInterval = MyConfig.config.getInterval(tieba.thread.run_interval)

    override suspend fun start() {
        super.start()
        MyConfig.config[tieba.thread.keywords].forEach { keyword ->
            launch {
                run(keyword)
            }
        }
    }

    /**
     * spider main logic
     */
    private suspend fun fetchThread(keyword: String) {
        val urlPrefix = "http://tieba.baidu.com/f?ie=utf-8&kw=${URLEncoder.encode(keyword, Charsets.UTF_8)}&pn="
        val visitedThreadSet = hashSetOf<Long>()
        /**
         *  todo <when time permits>
         *  scheduler load tasks, send requests to downloader and extract data (one coroutine per request)
         */
        for (pageNumber in 0..Int.MAX_VALUE step 50) {
            val url = "$urlPrefix$pageNumber"
            val resp = vertx.eventBus().requestAwait<JsonObject>(EventbusAddress.downloader,
                jsonObjectOf("url" to url)).body()
            val respText = resp.getString("body")
            println("got $url")
            /*
            get real DOM of this web page
             */
            val doc0 = Jsoup.parse(respText, url, Parser.htmlParser())
            val realHtml = doc0.select("code").joinToString {
                it.html()
                    .replace("<!--", "")
                    .replace("-->", "")
            }
            val doc = Jsoup.parse(realHtml, url, Parser.htmlParser())

            // extract data from web page
            val userList = hashMapOf<String, TiebaUser>()
            val threadList = doc.select("#thread_list > li.j_thread_list")
                .mapNotNull { thread ->
                    val updateTag = thread.select("div > div.col2_right.j_threadlist_li_right > div.threadlist_detail.clearfix > div.threadlist_author.pull_right > span.threadlist_reply_date.pull_right.j_reply_data").text()
                    val title = thread.select("div > div.col2_right.j_threadlist_li_right > div.threadlist_lz.clearfix > div.threadlist_title.pull_left.j_th_tit > a").text()
                    val data_fields = thread.attr("data-field")
                    val threadInfo = try {
                        JSON.parseObject(data_fields, ThreadInfo::class.java)
                    } catch (e: Exception) {
                        println("failed to parse json on $url")
                        println(data_fields)
                        e.printStackTrace()
                        return
                    }
                    userList[threadInfo.authorPortrait] =
                        TiebaUser(threadInfo.authorPortrait, threadInfo.authorNickname ?: threadInfo.authorName ?: "")
                    if (!visitedThreadSet.contains(threadInfo.id)) {
                        visitedThreadSet.add(threadInfo.id)
                        ThreadItem(updateTag, threadInfo, title)
                    } else {
                        null
                    }
                }
            if (threadList.isEmpty()) {
                println("end thread spider of keyword $keyword")
                break
            }
            /*
            save or update each thread
             */
            // bypass today's new thread
            val threadList2 = threadList.filter {
                !it.updateTag.contains(":")
            }
            CacheConnection.execute {
                upsertUsers(userList.values)
                upsertThread(threadList2, keyword)
            }
        }
    }

    private fun Transaction.upsertThread(threadList: List<ThreadItem>, keyword: String) {
        connection.prepareStatement(
            "insert into ${TiebaThreadTable.tableName}(" +
                "${TiebaThreadTable.threadId.name}, " +
                "${TiebaThreadTable.keyword.name}, " +
                "${TiebaThreadTable.title.name}, " +
                "${TiebaThreadTable.authorId.name}, " +
                "${TiebaThreadTable.replyNum.name}, " +
                "${TiebaThreadTable.lastReplyTag.name}) " +
                "values(?,?,?,?,?,?) " +
                "on conflict(${TiebaThreadTable.threadId.name}) " +
                "do update set " +
                "${TiebaThreadTable.replyNum.name}=excluded.${TiebaThreadTable.replyNum.name}, " +
                "${TiebaThreadTable.lastReplyTag.name}=excluded.${TiebaThreadTable.lastReplyTag.name}, " +
                "${TiebaThreadTable.updated.name}=0 " +
                "where ${TiebaThreadTable.replyNum.name}!=excluded.${TiebaThreadTable.replyNum.name}" +
                " or ${TiebaThreadTable.lastReplyTag.name}!=excluded.${TiebaThreadTable.lastReplyTag.name};")
            .apply {
                for (thread in threadList) {
                    setString(1, thread.threadInfo.id.toString())
                    setString(2, keyword)
                    setString(3, thread.title)
                    setString(4, thread.threadInfo.authorPortrait)
                    setInt(5, thread.threadInfo.replyNum)
                    setString(6, thread.updateTag)
                    addBatch()
                }
                executeBatch()
            }
    }

    suspend fun run(keyword: String) {

        while (true) {
            try {
                fetchThread(keyword)
            } catch (e: Exception) {
                println("error when fetch tieba thread")
                e.printStackTrace()
            }
            delay(threadUpdateInterval)
        }
    }

}