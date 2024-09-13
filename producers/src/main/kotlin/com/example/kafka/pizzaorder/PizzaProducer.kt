package com.example.kafka.pizzaorder

import com.github.javafaker.Faker
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import org.slf4j.LoggerFactory
import java.io.File
import java.util.Properties
import java.util.Random
import java.util.concurrent.ExecutionException

object PizzaProducer {
    private val logger = LoggerFactory.getLogger(PizzaProducer::class.java.name)

    internal fun sendPizzaMessage(
        kafkaProducer: KafkaProducer<String, String>,
        topicName: String, iterCnt: Int,
        interIntervalMillis: Long,
        intervalMillis: Long,
        intervalCount: Int,
        sync: Boolean
    ) {
        val pizzaMessage = PizzaMessage()

        var iterSeq = 0
        val seed = 2022L
        val random = Random(seed)
        val faker = Faker.instance(random)
        while (iterSeq++ != iterCnt) {
            val pMessage = pizzaMessage.produceMessage(faker, iterSeq)
            val record = ProducerRecord<String, String>(topicName, pMessage["key"], pMessage["message"])
            sendMessage(kafkaProducer, record, pMessage, sync)

            if (intervalCount > 0 && (iterSeq % intervalCount == 0)) {
                try {
                    logger.info("###### IntervalCount:$intervalCount intervalMillis:$intervalMillis ######")
                    Thread.sleep(intervalMillis)
                } catch (e: InterruptedException) {
                    logger.error(e.message)
                }
            }

            if (interIntervalMillis > 0) {
                try {
                    logger.info("interIntervalMillis:$interIntervalMillis")
                    Thread.sleep(interIntervalMillis)
                } catch (e: InterruptedException) {
                    logger.error(e.message)
                }
            }
        }
    }

    private fun sendMessage(
        kafkaProducer: KafkaProducer<String, String>,
        producerRecord: ProducerRecord<String, String>,
        pMessage: HashMap<String, String>,
        sync: Boolean
    ) {
        if (!sync) {
            kafkaProducer.send(producerRecord) { metadata, exception ->
                if (exception == null) {
                    logger.info("async message:${pMessage["key"]} partition: ${metadata.partition()} offset:${metadata.offset()}")
                } else {
                    logger.error("exception error from broker ${exception.message}")
                }
            }
        } else {
            try {
                val metadata = kafkaProducer.send(producerRecord).get()
                logger.info("sync message:${pMessage["key"]} partition: ${metadata.partition()} offset:${metadata.offset()}")
            } catch (e: ExecutionException) {
                logger.error(e.message)
            } catch (e: InterruptedException) {
                logger.error(e.message)
            }
        }
    }
}

fun main() {
    val topicName = "pizza-topic"
    val propertiesFile = "local.properties"

    val properties = Properties().apply {
        val localPropertiesFile = File(System.getProperty("user.dir"), propertiesFile)
        if (localPropertiesFile.exists()) {
            localPropertiesFile.inputStream().use { load(it) }
        }
    }

    val producerConfig = Properties().apply {
        setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, properties.getProperty("KAFKA_BOOTSTRAP_SERVERS"))
        setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.name)
        setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.name)
        setProperty(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "50000")
        // acks setting
        //  setProperty(ProducerConfig.ACKS_CONFIG, "0")
        // batch size: bytes
        //  setProperty(ProducerConfig.BATCH_SIZE_CONFIG, "32000")
        // linger ms
        //  setProperty(ProducerConfig.LINGER_MS_CONFIG, "5")
        // max block ms
        //  setProperty(ProducerConfig.MAX_BLOCK_MS_CONFIG, "60000")
        // retries
        //  setProperty(ProducerConfig.RETRIES_CONFIG, "3")
    }

    val kafkaProducer = KafkaProducer<String, String>(producerConfig)

    PizzaProducer.sendPizzaMessage(kafkaProducer, topicName, -1, 100, 1000, 100, true)
    kafkaProducer.close()
}