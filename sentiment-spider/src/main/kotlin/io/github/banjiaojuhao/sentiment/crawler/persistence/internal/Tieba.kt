package io.github.banjiaojuhao.sentiment.crawler.persistence.internal

import org.jetbrains.exposed.dao.IntIdTable
import org.jetbrains.exposed.sql.and
import org.jetbrains.exposed.sql.select

object TiebaThreadTable : IntIdTable() {
    override val tableName: String
        get() = "tieba_thread_table"
    val threadId = text("thread_id").uniqueIndex()
    val keyword = text("keyword")
    val title = text("title")
    val authorId = text("author_id")
    val replyNum = integer("reply_num")

    // identify whether thread updated(only fetch mm-dd not HH:mm)
    val lastReplyTag = text("last_reply_tag")

    val triedUpdateTime = long("_update_time").default(0)
    val updated = bool("_updated").default(false)
}

object TiebaPostTable : IntIdTable() {
    override val tableName: String
        get() = "tieba_post_table"
    val threadId = text("thread_id")
    val postId = text("post_id").uniqueIndex()
    val postNo = integer("post_no")
    val authorId = text("author_id")
    val commentNum = integer("comment_num")
    val replyDate = long("reply_date")
    val content = text("content")

    val syncTime = long("_sync_time").default(0)

    val triedUpdateTime = long("_update_time").default(0)
    val updated = bool("_updated").default(false)
}

object TiebaCommentTable : IntIdTable() {
    override val tableName: String
        get() = "tieba_comment_table"
    val threadId = text("thread_id")
    val postId = text("post_id").index()
    val authorId = text("author_id")
    val replyToId = text("reply_to_id")
    val content = text("content")
    val replyDate = long("reply_date")
    val _uniqueId = text("_unique_id").uniqueIndex()

    val syncTime = long("_sync_time").default(0)
}

object TiebaUserTable : IntIdTable() {
    override val tableName: String
        get() = "tieba_user_table"
    val userId = text("user_id").uniqueIndex()
    val nickname = text("nickname")
}

val filterPassThreadId = TiebaThreadTable.slice(TiebaThreadTable.threadId)
    .select {
        TiebaThreadTable.id inSubQuery (
            FilterTable.slice(FilterTable.recordId).select {
                (FilterTable.recordType eq FilterTable.RecordType.TiebaThread.toInt()) and
                    (FilterTable.state eq FilterTable.FilterState.FilterPass.toInt())
            })
    }