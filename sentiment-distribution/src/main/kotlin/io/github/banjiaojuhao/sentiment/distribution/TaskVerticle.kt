package io.github.banjiaojuhao.sentiment.distribution

import io.github.banjiaojuhao.sentiment.distribution.persistence.StoreConnection
import io.github.banjiaojuhao.sentiment.distribution.persistence.TaskTable
import io.github.banjiaojuhao.sentiment.persistence.ArticleTable
import io.github.banjiaojuhao.sentiment.persistence.SentenceWordsTable
import io.github.banjiaojuhao.sentiment.persistence.WordsTable
import io.vertx.core.json.JsonObject
import io.vertx.kotlin.core.json.jsonObjectOf
import io.vertx.kotlin.coroutines.CoroutineVerticle
import io.vertx.kotlin.coroutines.awaitBlocking
import io.vertx.kotlin.coroutines.toChannel
import kotlinx.coroutines.launch
import org.jetbrains.exposed.sql.*
import org.mapdb.DB
import org.mapdb.DBMaker
import org.mapdb.HTreeMap
import org.mapdb.Serializer

class TaskVerticle : CoroutineVerticle() {
    private lateinit var db: DB
    private lateinit var wordsMap: HTreeMap<String, Int>

    override suspend fun start() {
        super.start()
        db = DBMaker.fileDB("words-id.mapdb").make()
        wordsMap = db.hashMap("words", Serializer.STRING, Serializer.INTEGER).createOrOpen()

        val wordsIdList = StoreConnection.execute {
            WordsTable.selectAll().toList()
        }
        awaitBlocking {
            wordsIdList.forEach {
                wordsMap[it[WordsTable.word]] = it[WordsTable.id].value
            }
        }
        launch {
            getTask()
        }
        launch {
            saveResult()
        }
    }

    override suspend fun stop() {
        wordsMap.close()
        db.close()
        super.stop()
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

    private suspend fun getTask() {
        handlerWrapper("task.get") { params ->
            val count = params.getInteger("count", 1024)
            val timeNow = System.currentTimeMillis()
            val notExist = TaskTable.status.function("IS_NULL")
            val tasks = StoreConnection.execute {
                ArticleTable.leftJoin(TaskTable, { id }, { rid })
                    .slice(ArticleTable.id, ArticleTable.content, notExist)
                    .select {
                        (TaskTable.type eq TaskTable.TYPE.SENTIMENT_WORD.toInt()) and
                            ((TaskTable.status eq TaskTable.STATUS.NOT_PROCESS.toInt()) or TaskTable.status.isNull())
                    }.limit(count).toList().apply {
                        this.forEach { result: ResultRow ->
                            if (result[notExist] == 1) {
                                TaskTable.insert {
                                    it[type] = TaskTable.TYPE.SENTIMENT_WORD.toInt()
                                    it[rid] = result[ArticleTable.id].value
                                    it[status] = TaskTable.STATUS.PROCESSING.toInt()
                                    it[timestamp] = timeNow
                                }
                            } else {
                                TaskTable.update({
                                    TaskTable.type eq TaskTable.TYPE.SENTIMENT_WORD.toInt()
                                    TaskTable.rid eq result[ArticleTable.id].value
                                }) {
                                    it[status] = TaskTable.STATUS.PROCESSING.toInt()
                                    it[timestamp] = timeNow
                                }
                            }
                        }
                    }
            }.map {
                jsonObjectOf(
                    "id" to it[ArticleTable.id].value,
                    "sentence" to it[ArticleTable.content]
                )
            }
            jsonObjectOf(
                "task" to tasks
            )
        }
    }

    private data class Result(val sentenceId: Int, val words: List<String>, val sentiment: Int)

    private data class SentenceWord(val sentenceId: Int, val wordId: Int)

    private suspend fun saveResult() {
        handlerWrapper("result.save") { params ->
            val result = params.getJsonArray("result")
                ?.map {
                    it as JsonObject
                    Result(it.getInteger("id"),
                        it.getJsonArray("words").map { it as String }.filter {
                            it.length < 32
                        },
                        it.getInteger("sentiment"))
                } ?: throw IllegalArgumentException("result")
            storeResult(result)
            jsonObjectOf(
                "msg" to "ok"
            )
        }
    }

    private suspend fun storeResult(result: List<Result>) {
        val newWords = result.map { it.words }
            .flatten().toSet()
            .filterNot {
                wordsMap.contains(it)
            }

        StoreConnection.execute {
            newWords.map { _word ->
                WordsTable.insertAndGetId {
                    it[word] = _word
                }.value
            }
        }.zip(newWords).forEach {
            wordsMap[it.second] = it.first
        }
        db.commit()

        val splitWordsList = result.map { sentenceWords ->
            sentenceWords.words.map {
                SentenceWord(sentenceWords.sentenceId, wordsMap[it]!!)
            }
        }.flatten()

        val timeNow = System.currentTimeMillis()
        StoreConnection.execute {
            splitWordsList.forEach { rec ->
                SentenceWordsTable.insertIgnore {
                    it[sentenceId] = rec.sentenceId
                    it[wordId] = rec.wordId
                }
            }
            result.forEach { result: Result ->
                ArticleTable.update({
                    ArticleTable.id eq result.sentenceId
                }) {
                    it[sentiment] = result.sentiment
                }
                TaskTable.update({
                    TaskTable.type eq TaskTable.TYPE.SENTIMENT_WORD.toInt()
                    TaskTable.rid eq result.sentenceId
                }) {
                    it[status] = TaskTable.STATUS.PROCESSED.toInt()
                    it[timestamp] = timeNow
                }
            }
        }
    }

}
