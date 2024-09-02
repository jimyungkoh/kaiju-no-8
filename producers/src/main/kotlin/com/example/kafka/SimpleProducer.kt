package com.example.kafka

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import java.io.File
import java.util.Properties

object KafkaProducerApp {
    private const val TOPIC_NAME = "simple-topic"
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
        setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.name)
        setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.name)
    }

    fun sendMessage(message: String) {
        try {
            KafkaProducer<String, String>(createProducerConfig()).use { producer ->
                val record = ProducerRecord<String, String>(TOPIC_NAME, message)
                producer.send(record).get()
                println("Message sent successfully")
            }
        } catch (e: Exception) {
            println("An error occurred: ${e.message}")
            e.printStackTrace()
        }
    }
}

fun main() {
    KafkaProducerApp.sendMessage("hello kafka!")
}