package cn.banjiaojuhao.sentiment.crawler.postprocess

import cn.banjiaojuhao.sentiment.persistence.ArticleTable
import cn.banjiaojuhao.sentiment.persistence.SentenceWordsTable
import cn.banjiaojuhao.sentiment.crawler.persistence.external.StoreConnection
import cn.banjiaojuhao.sentiment.persistence.WordsTable
import io.vertx.ext.web.client.WebClient
import io.vertx.kotlin.core.json.jsonObjectOf
import io.vertx.kotlin.coroutines.CoroutineVerticle
import io.vertx.kotlin.coroutines.awaitBlocking
import io.vertx.kotlin.ext.web.client.sendJsonObjectAwait
import kotlinx.coroutines.*
import org.jetbrains.exposed.sql.*
import org.mapdb.DBMaker
import org.mapdb.Serializer

class SplitWordsVerticle : CoroutineVerticle() {

    private lateinit var webClient: WebClient

    private val db = DBMaker.fileDB("words-id.mapdb").make()
    private val wordsMap = db
        .hashMap("words", Serializer.STRING, Serializer.INTEGER)
        .createOrOpen()

    private data class Task(val id: Int, val sentence: String)

    private data class SplitResult(val sentenceId: Int, val words: List<String>)

    private data class SentenceWord(val sentenceId: Int, val wordId: Int)

    override suspend fun start() {
        super.start()
        webClient = WebClient.create(vertx)
        launch {
            run()
        }
    }

    override suspend fun stop() {
        db.close()
        webClient.close()
        super.stop()
    }

    private suspend fun run() {
        // refresh local words-id.mapdb cache
        val wordsIdList = StoreConnection.execute {
            WordsTable.selectAll().toList()
        }
        awaitBlocking {
            wordsIdList.forEach {
                wordsMap[it[WordsTable.word]] = it[WordsTable.id].value
            }
        }
        while (this.isActive) {
            val tasks = loadTask()
            split(tasks)?.let {
                storeResult(it)
            }
            delay(10_000L)
        }
    }

    private suspend fun storeResult(result: List<SplitResult>) {
        val validResult = result.map {
            SplitResult(it.sentenceId, it.words.filter { it.length < 32 })
        }
        val newWords = validResult.map { it.words }
            .flatten().toSet()
            .filterNot {
                wordsMap.contains(it)
            }

        StoreConnection.execute {
            upsertWords(newWords)
        }.zip(newWords).forEach {
            wordsMap[it.second] = it.first
        }

        val splitWordsList = validResult.map { sentenceWords ->
            sentenceWords.words.map {
                SentenceWord(sentenceWords.sentenceId, wordsMap[it]!!)
            }
        }.flatten()

        StoreConnection.execute {
            upsertSplitWords(splitWordsList)
        }
    }

    private suspend fun split(tasks: List<Task>): List<SplitResult>? {
        return tasks.map {
            if (!this.isActive) {
                return null
            }
            val words = webClient.postAbs("http://localhost:8082/split-words")
                .sendJsonObjectAwait(jsonObjectOf(
                    "sentence" to it.sentence
                )).bodyAsJsonObject().getJsonArray("words")
            SplitResult(it.id, words.map { it as String })
        }
    }

    private suspend fun loadTask(): List<Task> {
        val taskList = StoreConnection.execute {
            ArticleTable
                .slice(ArticleTable.id, ArticleTable.content)
                .select {
                    not(ArticleTable.id inSubQuery (
                        SentenceWordsTable.slice(SentenceWordsTable.wordId).selectAll().withDistinct()
                        ))
                }.limit(1000).map {
                    Task(it[ArticleTable.id].value, it[ArticleTable.content])
                }
        }

        return taskList
    }

    private fun upsertWords(words: List<String>): List<Int> {
        return words.map { _word ->
            WordsTable.insertAndGetId {
                it[word] = _word
            }.value
        }
    }

    private fun upsertSplitWords(data: List<SentenceWord>) {
        data.forEach { rec ->
            SentenceWordsTable.insertIgnore {
                it[sentenceId] = rec.sentenceId
                it[wordId] = rec.wordId
            }
        }

    }
}