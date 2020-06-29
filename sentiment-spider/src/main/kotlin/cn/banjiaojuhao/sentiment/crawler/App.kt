package cn.banjiaojuhao.sentiment.crawler

import cn.banjiaojuhao.sentiment.crawler.network.DownloaderVerticle
import cn.banjiaojuhao.sentiment.crawler.persistence.external.StoreConnection
import cn.banjiaojuhao.sentiment.crawler.persistence.internal.CacheConnection
import cn.banjiaojuhao.sentiment.crawler.postprocess.RestoreVerticle
import cn.banjiaojuhao.sentiment.crawler.spider.tieba.TiebaCommentVerticle
import cn.banjiaojuhao.sentiment.crawler.spider.tieba.TiebaPostVerticle
import cn.banjiaojuhao.sentiment.crawler.spider.tieba.TiebaThreadVerticle
import cn.banjiaojuhao.sentiment.crawler.spider.zhihu.ZhihuAnswerVerticle
import cn.banjiaojuhao.sentiment.crawler.spider.zhihu.ZhihuCommentVerticle
import cn.banjiaojuhao.sentiment.crawler.spider.zhihu.ZhihuTopicsVerticle
import com.fasterxml.jackson.module.kotlin.KotlinModule
import io.vertx.core.Vertx
import io.vertx.core.json.jackson.DatabindCodec
import io.vertx.kotlin.core.closeAwait
import io.vertx.kotlin.core.deployVerticleAwait
import io.vertx.kotlin.core.dns.addressResolverOptionsOf
import io.vertx.kotlin.core.undeployAwait
import io.vertx.kotlin.core.vertxOptionsOf
import io.vertx.kotlin.coroutines.awaitEvent
import kotlinx.coroutines.runBlocking
import sun.misc.Signal


/**
 * addresses used by verticles:
 *      DownloaderVerticle: sentiment.spider.downloader
 */


fun main() = runBlocking<Unit> {
    // add module for json parsing in spider
    DatabindCodec.mapper().registerModule(KotlinModule())
    // set faster dns for spider
    val vertx: Vertx = Vertx.vertx(vertxOptionsOf(
        addressResolverOptions = addressResolverOptionsOf(
            servers = listOf("1.2.4.8", "119.29.29.29"))
    ))
    val deployedVerticleIdList = arrayListOf<String>()

    deployedVerticleIdList.add(vertx.deployVerticleAwait(DownloaderVerticle()))
    deployedVerticleIdList.add(vertx.deployVerticleAwait(RestoreVerticle()))

    deployedVerticleIdList.add(vertx.deployVerticleAwait(TiebaThreadVerticle()))
    deployedVerticleIdList.add(vertx.deployVerticleAwait(TiebaPostVerticle()))
    deployedVerticleIdList.add(vertx.deployVerticleAwait(TiebaCommentVerticle()))

    deployedVerticleIdList.add(vertx.deployVerticleAwait(ZhihuTopicsVerticle()))
    deployedVerticleIdList.add(vertx.deployVerticleAwait(ZhihuAnswerVerticle()))
    deployedVerticleIdList.add(vertx.deployVerticleAwait(ZhihuCommentVerticle()))

    awaitEvent<Unit> { handler ->
        Signal.handle(Signal("INT")) {
            handler.handle(Unit)
        }
    }
    println("stop program")

    deployedVerticleIdList.asReversed().forEach {
        vertx.undeployAwait(it)
    }

    vertx.closeAwait()


    // Weibo is unavailable because of strict GET frequency limitation(1qps)
//    routineList.add(launch {
//        WeiboTopics.run("哈工大深圳")
//    })
//    routineList.add(launch {
//        WeiboComment.run()
//    })
//    routineList.add(launch {
//        WeiboLongContent.run()
//    })
//    routineList.add(launch {
//        WeiboSubComment.run()
//    })


    CacheConnection.close()
    StoreConnection.close()
    return@runBlocking
}
