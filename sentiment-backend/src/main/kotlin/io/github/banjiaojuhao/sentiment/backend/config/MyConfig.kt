package io.github.banjiaojuhao.sentiment.backend.config

import com.uchuhimo.konf.Config
import com.uchuhimo.konf.ConfigSpec
import com.uchuhimo.konf.Item
import com.uchuhimo.konf.source.toml


object store : ConfigSpec() {
    object mysql : ConfigSpec() {
        val url by required<String>()
        val username by required<String>()
        val password by required<String>()
    }
}

object login : ConfigSpec() {
    object session : ConfigSpec() {
        val expire by required<String>()
    }
}

object MyConfig {
    val config = Config {
        addSpec(store)
        addSpec(login)
    }.from.toml.file("sentiment-backend-config.toml")
}

fun Config.getInterval(time: Item<String>): Long {
    val timeText = this[time]
    val timeValue = timeText.substring(0, timeText.length - 1).toDoubleOrNull()
            ?: throw Exception("failed to parse time $timeText")
    val timeMultiplier = when (timeText[timeText.length - 1]) {
        'd' -> 24 * 3600_000L
        'h' -> 3600_000L
        'm' -> 60_000L
        's' -> 1000L
        else -> throw Exception("failed to parse time $timeText")
    }
    return (timeValue * timeMultiplier).toLong()
}
