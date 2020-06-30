package io.github.banjiaojuhao.sentiment.backend

import io.github.banjiaojuhao.sentiment.backend.config.MyConfig
import io.github.banjiaojuhao.sentiment.backend.config.getInterval
import io.github.banjiaojuhao.sentiment.backend.config.login
import io.github.banjiaojuhao.sentiment.backend.persistence.*
import io.github.banjiaojuhao.sentiment.persistence.*
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import io.vertx.kotlin.core.json.jsonArrayOf
import io.vertx.kotlin.core.json.jsonObjectOf
import io.vertx.kotlin.coroutines.CoroutineVerticle
import io.vertx.kotlin.coroutines.toChannel
import kotlinx.coroutines.launch
import org.jetbrains.exposed.sql.*
import org.jetbrains.exposed.sql.SqlExpressionBuilder.div
import java.util.*

class BackendVerticle : CoroutineVerticle() {
    override suspend fun start() {
        super.start()
        launch {
            login()
        }
        launch {
            info()
        }
        launch {
            articleList()
        }
        launch {
            sentiment()
        }
        launch {
            number()
        }
        launch {
            wordCloud()
        }
        launch {
            spammer()
        }
    }


    private suspend fun handlerWrapper(address: String, body: suspend (JsonObject) -> JsonObject) {
        val consumer = vertx.eventBus().consumer<JsonObject>(address)
        val channel = consumer.toChannel(vertx)
        for (msg in channel) {
            try {
                msg.reply(body(msg.body()))
            } catch (e: Exception) {
                msg.reply(jsonObjectOf(
                    "code" to 10000,
                    "message" to e.message
                ))
            }
        }
    }

    private suspend fun login() {
        val sessionExpire = MyConfig.config.getInterval(login.session.expire)
        handlerWrapper("backend.user.login") { params ->
            val username = params.getString("name")
            val password = params.getString("password")
            val logFail = DBConnection.execute {
                UserLoginInfo.slice(exists(UserLoginInfo.select {
                    (UserLoginInfo.username eq username) and
                        (UserLoginInfo.password eq password)
                })).selectAll().empty()
            }
            if (logFail) {
                jsonObjectOf(
                    "code" to 60204,
                    "message" to "Account and password are incorrect."
                )
            } else {
                var token: String
                while (true) {
                    token = UUID.randomUUID().toString()
                    DBConnection.execute {
                        LoginSession.insertIgnoreAndGetId {
                            it[sessionId] = token
                            it[LoginSession.username] = username
                            it[expireTime] = System.currentTimeMillis() + sessionExpire
                        }
                    } ?: continue
                    break
                }
                jsonObjectOf(
                    "code" to 20000,
                    "data" to jsonObjectOf(
                        "token" to token
                    )
                )
            }
        }
    }

    private suspend fun info() {
        handlerWrapper("backend.user.info") { params ->
            val token = params.getString("token")
            val resultRow = DBConnection.execute {
                LoginSession.leftJoin(UserBasicInfo, { username }, { username })
                    .select {
                        LoginSession.sessionId eq token
                    }.firstOrNull()
            } ?: return@handlerWrapper jsonObjectOf(
                "code" to 50008,
                "message" to "invalid token"
            )

            if (resultRow[LoginSession.expireTime] < System.currentTimeMillis()) {
                jsonObjectOf(
                    "code" to 50008,
                    "message" to "token expired"
                )
            } else {
                jsonObjectOf(
                    "code" to 20000,
                    "data" to jsonObjectOf(
                        "roles" to JsonArray(resultRow[UserBasicInfo.roles].split(","))
                    )
                )
            }
        }
    }

    private suspend fun articleList() {
        handlerWrapper("backend.article.list") { params ->
            val page = params.getInteger("page", 1) - 1
            val limit = params.getInteger("limit", 20)
            val platforms = params.getJsonArray("platforms", jsonArrayOf()).list.map { it as String }
            val sentiment = params.getJsonArray("sentiment", jsonArrayOf()).list.map { it as Int }
            val authorName = params.getString("author_name", "")
            val startTime = params.getLong("start_time", 0)
            val endTime = params.getLong("end_time", System.currentTimeMillis() / 1000L)

            var resultCount = -1
            val result = DBConnection.execute {
                ArticleTable.select {
                    ArticleTable
                        .time.between(startTime, endTime)
                        .and(ArticleTable.content neq "")
                        .run {
                            if (platforms.isNotEmpty()) {
                                this.and(ArticleTable.platform inList platforms)
                            } else {
                                this
                            }
                        }.run {
                            if (sentiment.isNotEmpty()) {
                                this.and(ArticleTable.sentiment inList sentiment)
                            } else {
                                this
                            }
                        }.run {
                            if (authorName != "") {
                                this.and(ArticleTable.authorName eq authorName)
                            } else {
                                this
                            }
                        }
                }.run {
                    resultCount = this.count()
                    this.orderBy(ArticleTable.time, SortOrder.DESC)
                        .limit(limit, page * limit)
                        .map {
                            jsonObjectOf(
                                "content" to it[ArticleTable.content],
                                "author" to it[ArticleTable.authorName],
                                "time" to it[ArticleTable.time],
                                "platform" to it[ArticleTable.platform],
                                "sentiment" to it[ArticleTable.sentiment],
                                "article_url" to it[ArticleTable.articleUrl],
                                "author_url" to it[ArticleTable.authorUrl]
                            )
                        }
                }
            }
            jsonObjectOf(
                "code" to 20000,
                "data" to jsonObjectOf(
                    "items" to JsonArray(result)
                ),
                "total" to resultCount
            )
        }
    }

    private suspend fun sentiment() {
        val oneDay = 24 * 3600L
        handlerWrapper("backend.statistics.sentiment") {
            val platform = it.getJsonArray("platforms", jsonArrayOf()).map { it as String }
            val sentimentList = it.getJsonArray("sentiment", jsonArrayOf(-1, 0, 1)).map { it as Int }
            val startTime = it.getLong("start_time")
            val endTime = it.getLong("end_time")
            val startDay = (startTime / oneDay).toInt()
            val endDay = (endTime / oneDay).toInt()

            val days = endDay - startDay + 1
            val result = arrayListOf<IntArray>(
                IntArray(if (-1 in sentimentList) days else 0),
                IntArray(if (0 in sentimentList) days else 0),
                IntArray(if (1 in sentimentList) days else 0)
            )
            val colDay = ((ArticleTable.time / oneDay).function("floor")).alias("day")
            val colCount = Count(ArticleTable.id).alias("count")
            DBConnection.execute {
                ArticleTable
                    .slice(ArticleTable.sentiment, colDay, colCount)
                    .select {
                        var expression = ArticleTable.time.between(startTime, endTime)
                        if (platform.isNotEmpty()) {
                            expression = expression.and(ArticleTable.platform inList platform)
                        }
//                            if (sentimentList.isNotEmpty()) {
//                                expression = expression.and(ArticleTable.sentiment inList sentimentList)
//                            }
                        expression
                    }.groupBy(ArticleTable.sentiment, colDay)
                    .orderBy(ArticleTable.sentiment)
                    .orderBy(colDay)
                    .toList()
            }.forEach {
                val sentiment = it[ArticleTable.sentiment]
                val day = it[colDay]!!.toInt()
                val count = it[colCount]
                result[sentiment + 1][day - startDay] = count
            }
            jsonObjectOf(
                "code" to 20000,
                "data" to JsonObject(
                    (0..2).map {
                        (it - 1).toString() to jsonObjectOf(
                            "items" to JsonArray(result[it].asList()),
                            "total" to result[it].sum())
                    }.toMap()
                )
            )
        }
    }

    private suspend fun number() {
        val oneDay = 24 * 3600L
        handlerWrapper("backend.statistics.number") { params ->
            val platformList = params.getJsonArray("platforms").map { it as String }
            val startTime = params.getLong("start_time")
            val endTime = params.getLong("end_time")
            val startDay = (startTime / oneDay).toInt()
            val endDay = (endTime / oneDay).toInt()

            val days = endDay - startDay + 1
            val result = platformList.map {
                it to IntArray(days)
            }.toMap()
            val colDay = ((ArticleTable.time / oneDay).function("floor")).alias("day")
            val colCount = Count(ArticleTable.id).alias("count")
            DBConnection.execute {
                ArticleTable
                    .slice(ArticleTable.platform, colDay, colCount)
                    .select {
                        ArticleTable.time.between(startTime, endTime) and (ArticleTable.platform inList platformList)
                    }.groupBy(ArticleTable.platform, colDay)
                    .orderBy(ArticleTable.platform)
                    .orderBy(colDay)
                    .toList()
            }.forEach { resultRow ->
                val platform = resultRow[ArticleTable.platform]
                val day = resultRow[colDay]!!.toInt()
                val count = resultRow[colCount]
                result[platform]!![day - startDay] = count
            }
            jsonObjectOf(
                "code" to 20000,
                "data" to JsonObject(
                    platformList.map {
                        it to jsonObjectOf(
                            "items" to JsonArray(result[it]!!.asList()),
                            "total" to result[it]!!.sum())
                    }.toMap()
                )
            )
        }
    }

    private suspend fun wordCloud() {
        handlerWrapper("backend.statistics.word-cloud") { params ->
            val top = params.getInteger("topk", 10)
            val platformList = params.getJsonArray("platforms", JsonArray()).map { it as String }
            val startTime = params.getLong("start_time", 0L)
            val endTime = params.getLong("end_time", System.currentTimeMillis() / 1000L)

            val colCount = Count(SentenceWordsTable.id)
            val resultSet = DBConnection.execute {
                SentenceWordsTable
                    .leftJoin(WordsTable, { wordId }, { WordsTable.id })
                    .slice(WordsTable.word,
                        colCount)
                    .select {
                        SentenceWordsTable.sentenceId inSubQuery (
                            ArticleTable.slice(ArticleTable.id)
                                .select {
                                    ArticleTable.time.between(startTime, endTime).run {
                                        if (platformList.isNotEmpty()) {
                                            this.and(ArticleTable.platform inList platformList)
                                        } else {
                                            this
                                        }
                                    }
                                })
                    }.groupBy(SentenceWordsTable.wordId)
                    .orderBy(colCount, SortOrder.DESC)
                    .limit(top)
                    .toList()
            }
            jsonObjectOf(
                "code" to 20000,
                "data" to JsonArray(resultSet.map {
                    jsonObjectOf(
                        "name" to it[WordsTable.word],
                        "value" to it[colCount]
                    )
                })
            )
        }
    }

    private suspend fun spammer() {
        handlerWrapper("backend.spammer") { params ->
            val page = params.getInteger("page", 1) - 1
            val limit = params.getInteger("limit", 10)
            val platformList = params.getJsonArray("platforms", JsonArray()).map { it as String }
            val startTime = params.getLong("start_time", 0L)
            val endTime = params.getLong("end_time", System.currentTimeMillis() / 1000L)

            val colCount = Count(ArticleTable.id)
            val colName = ArticleTable.authorName.function("max")
            val resultSet = DBConnection.execute {
                ArticleTable
                    .slice(ArticleTable.platform,
                        colName,
                        ArticleTable.authorUrl,
                        colCount)
                    .select {
                        ArticleTable.time.between(startTime, endTime)
                            .and(not(ArticleTable.authorUrl inSubQuery
                                WhiteListTable.slice(WhiteListTable.authorUrl).selectAll()))
                            .run {
                                if (platformList.isNotEmpty()) {
                                    this.and(ArticleTable.platform inList platformList)
                                } else {
                                    this
                                }
                            }
                    }.groupBy(ArticleTable.platform, ArticleTable.authorUrl)
                    .orderBy(colCount, SortOrder.DESC)
                    .limit(limit, page * limit)
                    .toList()
            }
            jsonObjectOf(
                "code" to 20000,
                "data" to JsonArray(resultSet.map {
                    jsonObjectOf(
                        "author_name" to it[colName],
                        "number" to it[colCount],
                        "platform" to it[ArticleTable.platform],
                        "author_url" to it[ArticleTable.authorUrl]
                    )
                })
            )
        }
    }
}