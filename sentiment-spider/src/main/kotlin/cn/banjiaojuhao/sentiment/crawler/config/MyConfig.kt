package cn.banjiaojuhao.sentiment.crawler.config

import com.uchuhimo.konf.Config
import com.uchuhimo.konf.ConfigSpec
import com.uchuhimo.konf.Item
import com.uchuhimo.konf.source.toml

object tieba : ConfigSpec() {
    object thread : ConfigSpec() {
        val keywords by required<List<String>>()
        val run_interval by required<String>()

        object filter : ConfigSpec() {
            val run_interval by required<String>()
        }
    }

    object post : ConfigSpec() {
        val run_interval by required<String>()
    }

    object comment : ConfigSpec() {
        val run_interval by required<String>()
        val refresh_interval by required<String>()
    }
}

object zhihu : ConfigSpec() {
    object topic : ConfigSpec() {
        val run_interval by required<String>()

    }

    object answer : ConfigSpec() {
        val run_interval by required<String>()
        val refresh_interval by required<String>()

    }

    object comment : ConfigSpec() {
        val run_interval by required<String>()
        val refresh_interval by required<String>()

    }
}

object store : ConfigSpec() {
    object mysql : ConfigSpec() {
        val url by required<String>()
        val username by required<String>()
        val password by required<String>()
    }
}

object MyConfig {
    val config = Config {
        addSpec(tieba)
        addSpec(zhihu)
        addSpec(store)
    }.from.toml.file("sentiment-spider-config.toml")

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
