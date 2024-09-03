package com.example.kafka.multipartition

import org.apache.kafka.clients.producer.Callback
import org.apache.kafka.clients.producer.RecordMetadata
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.lang.Exception

class CustomCallback(private val seq: Int) : Callback {
    companion object {
        private val logger: Logger = LoggerFactory.getLogger(ProducerAsyncWithKey::class.java.name)
    }

    override fun onCompletion(metadata: RecordMetadata?, exception: Exception?) {
        if (exception == null && metadata != null) {
            logger.debug(
                "seq:$seq; partition:${metadata.partition()}; offset:${metadata.offset()}"
            )
        } else {
            logger.error(
                "exception error from broker ${exception?.message}"
            )
        }
    }
}