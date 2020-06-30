package io.github.banjiaojuhao.sentiment.crawler.postprocess

import io.github.banjiaojuhao.sentiment.persistence.ArticleTable
import io.github.banjiaojuhao.sentiment.crawler.persistence.external.StoreConnection
import io.github.banjiaojuhao.sentiment.crawler.persistence.internal.*
import io.github.banjiaojuhao.sentiment.crawler.persistence.internal.TiebaPostTable.threadId
import io.github.banjiaojuhao.sentiment.crawler.persistence.updatePatch
import io.vertx.kotlin.coroutines.CoroutineVerticle
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import org.jetbrains.exposed.sql.*

class RestoreVerticle : CoroutineVerticle() {
    private val sentimentRange = listOf(-1, 0, 1)

    override suspend fun start() {
        super.start()
        launch {
            TiebaThreadFilter.run()
        }
        launch {
            run()
        }
    }

    suspend fun run() {
        while (true) {
            delay(10_000L)

            restoreTiebaPost()
            restoreTiebaComment()

            restoreZhihuAnswer()
            restoreZhihuComment()
        }
    }

    private suspend fun restoreTiebaPost() {
        val updateTag = System.currentTimeMillis()
        val dataSet: List<ResultRow> = CacheConnection.execute {
            val updatedItems = TiebaPostTable.updatePatch({
                (TiebaPostTable.syncTime greaterEq 0L) and
                    (threadId inSubQuery filterPassThreadId)
            }, 1000) {
                it[syncTime] = updateTag
            }
            if (updatedItems == 0) {
                return@execute emptyList()
            }
            TiebaPostTable
                .leftJoin(TiebaUserTable, { authorId }, { userId })
                .leftJoin(TiebaThreadTable, { threadId }, { threadId })
                .select {
                    TiebaPostTable.syncTime eq updateTag
                }.toList()
        }
        if (dataSet.isEmpty()) {
            return
        }
        StoreConnection.execute {
            ArticleTable.batchInsert(dataSet, ignore = true) {
                this[ArticleTable.articleIdTabled] = "T" + it[TiebaPostTable.postId]
                this[ArticleTable.platform] = "TB" + it[TiebaThreadTable.keyword]
                this[ArticleTable.articleType] = 1
                this[ArticleTable.articleUrl] = "https://tieba.baidu.com/p/${it[threadId]}#post_content_${it[TiebaPostTable.postId]}"
                this[ArticleTable.content] = it[TiebaPostTable.content]
                this[ArticleTable.authorName] = it[TiebaUserTable.nickname]
                this[ArticleTable.authorId] = it[TiebaPostTable.authorId]
                this[ArticleTable.authorUrl] = "https://tieba.baidu.com/home/main?id=" + it[TiebaPostTable.authorId]
                // todo this field is set and updated when inserting comments
                this[ArticleTable.heat] = 0
                this[ArticleTable.time] = it[TiebaPostTable.replyDate]
                this[ArticleTable.sentiment] = sentimentRange.random()
            }
        }
        CacheConnection.execute {
            TiebaPostTable.update({
                TiebaPostTable.syncTime eq updateTag
            }) {
                it[syncTime] = -1
            }
        }
    }

    private suspend fun restoreTiebaComment() {
        val updateTag = System.currentTimeMillis()
        val dataSet: List<ResultRow> = CacheConnection.execute {
            val updatedItems = TiebaCommentTable.updatePatch({
                (TiebaCommentTable.syncTime greaterEq 0L) and
                    (TiebaCommentTable.threadId inSubQuery filterPassThreadId)
            }, 1000) {
                it[syncTime] = updateTag
            }
            if (updatedItems == 0) {
                return@execute emptyList()
            }
            TiebaCommentTable
                .leftJoin(TiebaUserTable, { authorId }, { userId })
                .leftJoin(TiebaThreadTable, { TiebaCommentTable.threadId }, { threadId })
                .select {
                    TiebaCommentTable.syncTime eq updateTag
                }.toList()
        }
        if (dataSet.isEmpty()) {
            return
        }
        StoreConnection.execute {
            ArticleTable.batchInsert(dataSet, ignore = true) {
                this[ArticleTable.articleIdTabled] = "T" + it[TiebaCommentTable.id]
                this[ArticleTable.platform] = "TB" + it[TiebaThreadTable.keyword]
                this[ArticleTable.articleType] = 2
                this[ArticleTable.articleUrl] = "https://tieba.baidu.com/p/${it[TiebaCommentTable.threadId]}#post_content_${it[TiebaCommentTable.postId]}"
                this[ArticleTable.content] = it[TiebaCommentTable.content]
                this[ArticleTable.authorName] = it[TiebaUserTable.nickname]
                this[ArticleTable.authorId] = it[TiebaCommentTable.authorId]
                this[ArticleTable.authorUrl] = "https://tieba.baidu.com/home/main?id=" + it[TiebaCommentTable.authorId]
                this[ArticleTable.heat] = 0
                this[ArticleTable.time] = it[TiebaCommentTable.replyDate]
                this[ArticleTable.sentiment] = sentimentRange.random()
            }
        }
        CacheConnection.execute {
            TiebaCommentTable.update({
                TiebaCommentTable.syncTime eq updateTag
            }) {
                it[syncTime] = -1
            }
        }
    }

    private suspend fun restoreZhihuAnswer() {
        val updateTag = System.currentTimeMillis()
        val dataSet: List<ResultRow> = CacheConnection.execute {
            val updatedItems = ZhihuAnswerTable.updatePatch({
                ZhihuAnswerTable.syncTime greaterEq 0L
            }, 1000) {
                it[syncTime] = updateTag
            }
            if (updatedItems == 0) {
                return@execute emptyList()
            }
            ZhihuAnswerTable
                .leftJoin(ZhihuUserTable, { authorId }, { userId })
                .select {
                    ZhihuAnswerTable.syncTime eq updateTag
                }.toList()
        }
        if (dataSet.isEmpty()) {
            return
        }
        StoreConnection.execute {
            ArticleTable.batchInsert(dataSet, ignore = true) {
                this[ArticleTable.articleIdTabled] = "Z" + it[ZhihuAnswerTable.answerId]
                this[ArticleTable.platform] = "ZH"
                this[ArticleTable.articleType] = 1
                this[ArticleTable.articleUrl] = "https://www.zhihu.com/question/${it[ZhihuAnswerTable.questionId]}/answer/${it[ZhihuAnswerTable.answerId]}"
                this[ArticleTable.content] = it[ZhihuAnswerTable.excerpt]
                this[ArticleTable.authorName] = it[ZhihuUserTable.name]
                this[ArticleTable.authorId] = it[ZhihuAnswerTable.authorId]
                this[ArticleTable.authorUrl] = "https://www.zhihu.com/people/" + it[ZhihuAnswerTable.authorId]
                this[ArticleTable.heat] = 0
                this[ArticleTable.time] = it[ZhihuAnswerTable.updateTime]
                this[ArticleTable.sentiment] = sentimentRange.random()
            }
        }
        CacheConnection.execute {
            ZhihuAnswerTable.update({
                ZhihuAnswerTable.syncTime eq updateTag
            }) {
                it[syncTime] = -1
            }
        }
    }

    private suspend fun restoreZhihuComment() {
        val updateTag = System.currentTimeMillis()
        val dataSet: List<ResultRow> = CacheConnection.execute {
            val updatedItems = ZhihuCommentTable.updatePatch({
                ZhihuCommentTable.syncTime greaterEq 0L
            }, 1000) {
                it[syncTime] = updateTag
            }
            if (updatedItems == 0) {
                return@execute emptyList()
            }
            ZhihuCommentTable
                .leftJoin(ZhihuUserTable, { authorId }, { userId })
                .select {
                    ZhihuCommentTable.syncTime eq updateTag
                }.toList()
        }
        if (dataSet.isEmpty()) {
            return
        }
        StoreConnection.execute {
            ArticleTable.batchInsert(dataSet, ignore = true) {
                this[ArticleTable.articleIdTabled] = "Z" + it[ZhihuCommentTable.commentId]
                this[ArticleTable.platform] = "ZH"
                this[ArticleTable.articleType] = 2
                this[ArticleTable.articleUrl] = "https://www.zhihu.com/question/${it[ZhihuCommentTable.questionId]}" + if (it[ZhihuCommentTable.isToAnswer]) {
                    "/answer/${it[ZhihuCommentTable.parentId]}"
                } else {
                    ""
                }
                this[ArticleTable.content] = it[ZhihuCommentTable.content]
                this[ArticleTable.authorName] = it[ZhihuUserTable.name]
                this[ArticleTable.authorId] = it[ZhihuCommentTable.authorId]
                this[ArticleTable.authorUrl] = "https://www.zhihu.com/people/" + it[ZhihuCommentTable.authorId]
                this[ArticleTable.heat] = 0
                this[ArticleTable.time] = it[ZhihuCommentTable.createTime]
                this[ArticleTable.sentiment] = sentimentRange.random()
            }
        }
        CacheConnection.execute {
            ZhihuCommentTable.update({
                ZhihuCommentTable.syncTime eq updateTag
            }) {
                it[syncTime] = -1
            }
        }
    }
}