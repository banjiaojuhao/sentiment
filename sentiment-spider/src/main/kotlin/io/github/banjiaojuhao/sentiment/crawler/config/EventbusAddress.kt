package io.github.banjiaojuhao.sentiment.crawler.config

object EventbusAddress {
    /**
     *  only text response body will be returned to caller.
     *  params:
     *      method<String?>: GET(default), POST
     *      url<String>: http://example.com/resource
     *      headers<Map<String, String>?>: headers of this request
     *      body<String?>: body text
     *      task_tag<String?>: requests with same task_tag will be send by same proxy
     *  return:
     *      status_code<Int>: response status code
     *      headers<Map<String, String>>:
     *      body<String>:
     *      proxy_id<Long>: id of proxy sending current request, used to adjust proxy weight
     */
    const val downloader = "sentiment.spider.downloader"

    /**
     *  is response returned by proxy_id correct
     *  params:
     *      proxy_id<Long>:
     *      correct<Boolean>:
     */
    const val proxyState = "sentiment.spider.downloader.proxy"
}