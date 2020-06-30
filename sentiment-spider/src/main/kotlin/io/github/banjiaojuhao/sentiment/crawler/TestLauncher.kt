package io.github.banjiaojuhao.sentiment.crawler

import io.github.banjiaojuhao.sentiment.crawler.config.EventbusAddress
import io.github.banjiaojuhao.sentiment.crawler.network.DownloaderVerticle
import io.vertx.core.Vertx
import io.vertx.core.json.JsonObject
import io.vertx.kotlin.core.deployVerticleAwait
import io.vertx.kotlin.core.eventbus.requestAwait
import io.vertx.kotlin.core.json.jsonObjectOf
import io.vertx.kotlin.core.undeployAwait
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking

fun main() {
    runBlocking<Unit> {
        launch {

        }
        val vertx = Vertx.vertx()
        val downloaderVerticle = DownloaderVerticle()
        val id = vertx.deployVerticleAwait(downloaderVerticle)
        launch {
            delay(500L)
            vertx.eventBus().requestAwait<JsonObject>(EventbusAddress.downloader,
            jsonObjectOf("url" to "http://baidu.com/"))
        }
//    val result = tiebaCommentVerticle.fetchComment(TiebaCommentVerticle.CommentTask("6642287088", "132363048274"))
//    println(result)
        delay(1000L)
        vertx.undeployAwait(id)
    }
}