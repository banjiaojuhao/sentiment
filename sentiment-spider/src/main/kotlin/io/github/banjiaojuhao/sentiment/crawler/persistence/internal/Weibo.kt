package io.github.banjiaojuhao.sentiment.crawler.persistence.internal

import org.jetbrains.exposed.dao.IntIdTable

object WeiboPostTable : UpdateInfoTable() {
    override val tableName: String
        get() = "weibo_post_table"
    val postId = text("post_id").uniqueIndex()
    val authorId = text("author_id")
    val createTime = long("create_time")
    val content = text("content")
    val needUpdateContent = bool("need_update_content")
    val repostCount = integer("repost_count")
    val commentCount = integer("comment_count")
    val voteupCount = integer("voteup_count")
}

object WeiboCommentTable : UpdateInfoTable() {
    override val tableName: String
        get() = "weibo_comment_table"
    val postId = text("post_id")
    val commentId = text("comment_id").uniqueIndex()
    val authorId = text("author_id")
    val createTime = long("create_time")
    val content = text("content")
    val voteupCount = integer("voteup_count")
    val subCommentCount = integer("subcomment_count")
}

object WeiboSubCommentTable : UpdateInfoTable() {
    override val tableName: String
        get() = "weibo_sub_comment_table"
    val commentId = text("comment_id")
    val subCommentId = text("sub_comment_id").uniqueIndex()
    val authorId = text("author_id")
    val createTime = long("create_time")
    val content = text("content")
    val voteupCount = integer("voteup_count")
}

object WeiboUserTable : IntIdTable() {
    override val tableName: String
        get() = "weibo_user_table"
    val userId = text("user_id").uniqueIndex()
    val userName = text("user_name")
}