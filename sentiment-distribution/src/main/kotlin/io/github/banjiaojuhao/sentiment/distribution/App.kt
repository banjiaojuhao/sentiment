package io.github.banjiaojuhao.sentiment.distribution

import io.github.banjiaojuhao.sentiment.distribution.persistence.StoreConnection
import io.vertx.core.Vertx
import io.vertx.core.http.HttpMethod
import io.vertx.core.json.JsonObject
import io.vertx.kotlin.core.closeAwait
import io.vertx.kotlin.core.deployVerticleAwait
import io.vertx.kotlin.core.eventbus.deliveryOptionsOf
import io.vertx.kotlin.core.eventbus.requestAwait
import io.vertx.kotlin.core.http.httpServerOptionsOf
import io.vertx.kotlin.core.http.listenAwait
import io.vertx.kotlin.core.undeployAwait
import io.vertx.kotlin.coroutines.CoroutineVerticle
import io.vertx.kotlin.coroutines.awaitEvent
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import sun.misc.Signal

class MainVerticle : CoroutineVerticle() {
    override suspend fun start() {
        super.start()

        val server = vertx.createHttpServer(httpServerOptionsOf(compressionSupported = true))
        server.requestHandler { request ->
            if (request.method() != HttpMethod.POST) {
                request.response()
                    .setStatusCode(405)
                    .end("Method not Allowed")
            } else {
                launch {
                    val address = request.path().substringAfter("/").replace('/', '.')
                    try {
                        val data = awaitEvent<JsonObject> {
                            request.bodyHandler { buffer ->
                                it.handle(buffer.toJsonObject())
                            }
                        }
                        println("request $data on address $address")
                        val result = vertx.eventBus().requestAwait<JsonObject>(address, data,
                            deliveryOptionsOf(sendTimeout = 600_000))
                        request.response()
                            .setStatusCode(200)
                            .putHeader("Content-Type", "application/json;charset=UTF-8")
                            .end(result.body().toBuffer())
                    } catch (e: Exception) {
                        request.response()
                            .setStatusCode(500)
                            .end(e.message)
                    }
                }
            }
        }
        server.listenAwait(8090)
    }
}


fun main() = runBlocking<Unit> {
    val vertx: Vertx = Vertx.vertx()
    val deployedVerticleIdList = arrayListOf<String>()

    deployedVerticleIdList.add(vertx.deployVerticleAwait(MainVerticle()))
    deployedVerticleIdList.add(vertx.deployVerticleAwait(TaskVerticle()))

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
    StoreConnection.close()
    return@runBlocking
}
