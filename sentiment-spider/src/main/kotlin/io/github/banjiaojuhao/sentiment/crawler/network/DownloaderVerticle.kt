package io.github.banjiaojuhao.sentiment.crawler.network

import io.github.banjiaojuhao.sentiment.crawler.config.EventbusAddress
import io.vertx.core.MultiMap
import io.vertx.core.buffer.Buffer
import io.vertx.core.eventbus.Message
import io.vertx.core.json.JsonObject
import io.vertx.ext.web.client.HttpResponse
import io.vertx.ext.web.client.WebClient
import io.vertx.kotlin.core.json.jsonObjectOf
import io.vertx.kotlin.coroutines.CoroutineVerticle
import io.vertx.kotlin.coroutines.toChannel
import io.vertx.kotlin.ext.web.client.sendAwait
import io.vertx.kotlin.ext.web.client.sendBufferAwait
import io.vertx.kotlin.ext.web.client.webClientOptionsOf
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import java.security.InvalidParameterException

const val UA_Win_Chrome = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.106 Safari/537.36"
const val UA_Android = "Mozilla/5.0 (Linux; Android 4.4.2; Nexus 4 Build/KOT49H) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/34.0.1847.114 Mobile Safari/537.36"

/**
 * Current downloader verticle just limit request frequency of
 * domain to ensure running correctly. <br>
 * Proxy will be added to accelerate after proxy service finished. <br>
 * Proxy service are written using golang. Resend request to random proxy.
 */
class DownloaderVerticle : CoroutineVerticle() {
    private val webClient: WebClient by lazy {
        val webClientOptions = webClientOptionsOf(
            userAgent = UA_Win_Chrome,
            keepAlive = true,
            trustAll = true,
            verifyHost = false,
            ssl = true,
            tryUseCompression = true
//        , proxyOptions = proxyOptionsOf(host = "127.0.0.1", port = 8888)
        )
        WebClient.create(vertx, webClientOptions)
    }

    override suspend fun start() {
        super.start()

        launch {
            run()
        }
    }

    private suspend fun run() {
        val consumer = vertx.eventBus().consumer<JsonObject>(EventbusAddress.downloader)
        val channel = consumer.toChannel(vertx)

        val subRequestQueue = hashMapOf<String, Channel<Message<JsonObject>>>()

        // todo there is no need to use subRequestQueue after having proxy service
        suspend fun requestHandlerForHost(host: String) {
            for (msg in subRequestQueue[host]!!) {
                try {
                    val params = msg.body()
                    val method = params.getString("method", "GET")
                    val url = params.getString("url") ?: throw InvalidParameterException("url")
                    val headers = params.getJsonObject("headers")
                    val body = params.getString("body", "")
                    val request = when (method) {
                        "GET" -> {
                            webClient.getAbs(url)
                        }
                        "POST" -> {
                            webClient.postAbs(url)
                        }
                        else -> {
                            throw InvalidParameterException("method($method)")
                        }
                    }
                    if (headers?.containsKey("User-Agent") != true
                        && url.contains("://m.")) {
                        request.putHeader("User-Agent", UA_Android)
                    }
                    headers?.let {
                        request.putHeaders(headers.map as MultiMap)
                    }
                    // todo set timeout larger when using real proxy
                    request.timeout(5_000L)
                    var response: HttpResponse<Buffer>? = null
                    for (i in 0..2) {
                        try {
                            val tmpRequest = request.copy()
                            if (!this.isActive) {
                                msg.fail(3, "stop program")
                                return
                            }
                            response = when (method) {
                                "GET" -> {
                                    tmpRequest.sendAwait()
                                }
                                "POST" -> {
                                    tmpRequest.sendBufferAwait(Buffer.buffer(body))
                                }
                                else -> {
                                    throw InvalidParameterException("method($method)")
                                }
                            }
                            break
                        } catch (e: IllegalStateException) {
                            if (e.message?.contains("Client is closed") == true) {
                                msg.fail(3, "verticle stopped")
                            }
                        } catch (e: Exception) {
                            e.printStackTrace()
                        }
                    }
                    if (response != null) {
                        val responseHeaders = hashMapOf<String, String>()
                        response.headers().forEach {
                            responseHeaders[it.key] = it.value
                        }
                        msg.reply(jsonObjectOf(
                            "status_code" to response.statusCode(),
                            "headers" to responseHeaders,
                            "body" to response.bodyAsString(),
                            // todo set real proxy_id
                            "proxy_id" to 0
                        ))
                    } else {
                        msg.fail(2, "failed to get response")
                    }
                    delay(500L)
                } catch (e: InvalidParameterException) {
                    msg.fail(1, "invalid param ${e.message}")
                } catch (e: Exception) {
                    msg.fail(0, e.toString())
                }
            }
        }

        for (msg in channel) {
            launch {
                val params = msg.body()
                val host = params.getString("url").substringAfter("://").substringBefore("/")
                if (host !in subRequestQueue) {
                    subRequestQueue[host] = Channel()
                    launch {
                        requestHandlerForHost(host)
                    }
                }
                subRequestQueue[host]!!.send(msg)
            }
        }
    }
}