package io.github.banjiaojuhao.sentiment.distribution.client

import io.vertx.core.Vertx
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import io.vertx.ext.web.client.WebClient
import io.vertx.kotlin.core.closeAwait
import io.vertx.kotlin.core.deployVerticleAwait
import io.vertx.kotlin.core.json.jsonObjectOf
import io.vertx.kotlin.core.undeployAwait
import io.vertx.kotlin.coroutines.CoroutineVerticle
import io.vertx.kotlin.coroutines.awaitEvent
import io.vertx.kotlin.ext.web.client.sendJsonObjectAwait
import kotlinx.coroutines.*
import sun.misc.Signal


fun main() = runBlocking<Unit> {
    val vertx: Vertx = Vertx.vertx()
    val deployedVerticleIdList = arrayListOf<String>()

    deployedVerticleIdList.add(vertx.deployVerticleAwait(MainVerticle()))

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
    return@runBlocking
}


class MainVerticle : CoroutineVerticle() {
    private lateinit var webClient: WebClient

    override suspend fun start() {
        super.start()
//        webClientOptionsOf(keyStoreOptions = jksOptionsOf())
        webClient = WebClient.create(vertx)

        launch {
            main()
        }
    }

    private suspend fun main() {
        while (isActive) {
            val tasks = webClient.postAbs("http://121.40.55.44:8090/task/get")
                .sendJsonObjectAwait(jsonObjectOf())
                .bodyAsJsonObject().getJsonArray("task")
            if (tasks.isEmpty) {
                delay(600_000L)
                continue
            }

            val idList = async {
                tasks.map {
                    it as JsonObject
                    it.getInteger("id")
                }
            }
            val splitWord = async {
                webClient.postAbs("http://localhost:8082/split-words")
                    .sendJsonObjectAwait(jsonObjectOf("sentence" to tasks))
                    .bodyAsJsonObject()
                    .getJsonArray("result").map {
                        it as JsonObject
                        it.getInteger("id")!! to it.getJsonArray("words").mapNotNull {
                            it as String
                            if (it.length < 32) {
                                null
                            } else {
                                it
                            }
                        }
                    }.toMap()
            }
            val predict = async {
                webClient.postAbs("http://localhost:8083/predict")
                    .sendJsonObjectAwait(jsonObjectOf("sentence" to tasks))
                    .bodyAsJsonObject()
                    .getJsonArray("result").map {
                        it as JsonObject
                        it.getInteger("id")!! to it.getString("sentiment")!!.toInt()
                    }.toMap()
            }

            val predictResult = predict.await()
            val splitWordResult = splitWord.await()
            val result = idList.await().map {
                jsonObjectOf(
                    "id" to it,
                    "sentiment" to (predictResult[it] ?: error("")),
                    "words" to JsonArray(splitWordResult[it] ?: error(""))
                )
            }
            webClient.postAbs("http://121.40.55.44:8090/result/save")
                .sendJsonObjectAwait(jsonObjectOf("result" to JsonArray(result)))
        }

    }
}