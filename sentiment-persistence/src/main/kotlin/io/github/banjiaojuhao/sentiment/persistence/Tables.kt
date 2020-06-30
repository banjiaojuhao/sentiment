package io.github.banjiaojuhao.sentiment.persistence

import org.jetbrains.exposed.dao.IntIdTable

object ArticleTable : IntIdTable() {
    override val tableName: String
        get() = "article"
    val articleIdTabled = varchar("article_id", 100).uniqueIndex()
    val platform = text("platform")
    val articleType = integer("article_type")
    val articleUrl = text("article_url")
    val content = text("content")
    val authorName = text("author_name")
    val authorId = text("author_id")
    val authorUrl = text("author_url")
    val heat = integer("heat")
    val time = long("time")
    val sentiment = integer("sentiment")
}

object WhiteListTable : IntIdTable() {
    override val tableName: String
        get() = "whitelist"
    val authorUrl = text("author_url")
}

object WordsTable : IntIdTable() {
    override val tableName: String
        get() = "words"
    val word = varchar("word", 32, "utf8mb4_bin").uniqueIndex()
}

object SentenceWordsTable : IntIdTable() {
    override val tableName: String
        get() = "sentence_words"
    val sentenceId = integer("sentence_id").references(ArticleTable.id).index()
    val wordId = integer("word_id").references(WordsTable.id)

    init {
        uniqueIndex(sentenceId, wordId)
    }
}

object UserLoginInfo : IntIdTable() {
    override val tableName: String
        get() = "user_login_info"
    val username = varchar("username", 100).uniqueIndex()
    val password = text("password")
}

object UserBasicInfo : IntIdTable() {
    override val tableName: String
        get() = "user_basic_info"
    val roles = text("roles")
    val department = text("department")
    val phone = text("phone")
    val username = varchar("username", 100).references(UserLoginInfo.username).uniqueIndex()
}

object LoginSession : IntIdTable() {
    override val tableName: String
        get() = "login_session"
    val sessionId = varchar("session_id", 100).uniqueIndex()
    val expireTime = long("expire_time")
    val username = varchar("username", 100).references(UserLoginInfo.username)
}