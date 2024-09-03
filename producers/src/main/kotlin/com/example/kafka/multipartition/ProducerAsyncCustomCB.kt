package com.example.kafka.multipartition

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.IntegerSerializer
import org.apache.kafka.common.serialization.StringSerializer
import org.slf4j.LoggerFactory
import java.io.File
import java.util.Properties

object ProducerAsyncCustomCB {
    private val logger = LoggerFactory.getLogger(ProducerAsyncCustomCB::class.java.name)
    private const val TOPIC_NAME = "multipart-topic"
    private const val PROPERTIES_FILE = "local.properties"

    private val properties: Properties by lazy {
        Properties().apply {
            val localPropertiesFile = File(System.getProperty("user.dir"), PROPERTIES_FILE)
            if (localPropertiesFile.exists()) {
                localPropertiesFile.inputStream().use { load(it) }
            }
        }
    }

    private fun createProducerConfig(): Properties = Properties().apply {
        setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, properties.getProperty("KAFKA_BOOTSTRAP_SERVERS"))
        setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer::class.java.name)
        setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.name)
    }

    fun sendMessage(message: String) {
        try {
            KafkaProducer<Int, String>(createProducerConfig()).use { producer ->
                for (key in 0..20){
                    val record = ProducerRecord(TOPIC_NAME, key, message)
                    val callback = CustomCallback(key)
                    producer.send(record, callback)
                }
            }

            logger.info("Message sent successfully")
        } catch (e: Exception) {
            logger.error("An error occurred: ${e.message}")
            e.printStackTrace()
        }
    }
}

fun main() {
    ProducerAsyncCustomCB.sendMessage("hello kafka!")
}