package io.github.banjiaojuhao.sentiment.crawler.persistence.internal

import org.jetbrains.exposed.dao.IntIdTable

object ZhihuQuestionTable : UpdateInfoTable() {
    override val tableName: String
        get() = "zhihu_question_table"
    val questionId = text("question_id").uniqueIndex()
    val authorId = text("author_id").default("")
    val createTime = long("create_time").default(0)
    val answerCount = integer("answer_count").default(0)
    val commentCount = integer("comment_count").default(0)
    val followerCount = integer("follower_count").default(0)
    val title = text("title").default("")
}

object ZhihuAnswerTable : UpdateInfoTable() {
    override val tableName: String
        get() = "zhihu_answer_table"
    val questionId = text("question_id")
    val answerId = text("answer_id").uniqueIndex()
    val authorId = text("author_id")
    val createTime = long("create_time")
    val updateTime = long("update_time")
    val voteupCount = integer("voteup_count")
    val commentCount = integer("comment_count")
    val latestCommentTime = long("latest_comment_time").default(0)
    val content = text("content")
    val excerpt = text("excerpt")

    val syncTime = long("_sync_time").default(0)
}

object ZhihuCommentTable : IntIdTable() {
    override val tableName: String
        get() = "zhihu_comment_table"
    val questionId = text("question_id")
    val commentId = text("comment_id").uniqueIndex()
    val authorId = text("author_id")
    val isToAnswer = bool("is_to_answer")

    // question or answer id
    val parentId = text("parent_id")
    val content = text("content")
    val createTime = long("create_time")
    val voteCount = integer("vote_count")

    // "null" represents no reply to user
    val replyToAuthorId = text("reply_to_author_id").default("null")

    val syncTime = long("_sync_time").default(0)
}

object ZhihuUserTable : IntIdTable() {
    override val tableName: String
        get() = "zhihu_user_table"
    val userId = text("user_id").uniqueIndex()
    val urlToken = text("url_token")
    val name = text("name")
}