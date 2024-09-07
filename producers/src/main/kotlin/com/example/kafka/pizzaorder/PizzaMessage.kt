package com.example.kafka.pizzaorder

import com.github.javafaker.Faker
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.*
import kotlin.collections.HashMap
import java.util.Random

class PizzaMessage {
    companion object {
        private val pizzaNames =
            listOf("Potato Pizza", "Cheese Pizza", "Cheese Garlic Pizza", "Super Supreme Pizza", "Peperoni Pizza")
        private val pizzaShop = listOf(
            "A001",
            "B001",
            "C001",
            "D001",
            "E001",
            "F001",
            "G001",
            "H001",
            "I001",
            "J001",
            "K001",
            "L001",
            "M001",
            "N001",
            "O001",
            "P001",
            "Q001"
        )
    }

    // 인자로 피자명 또는 피자가게 List를 입력 받아서 랜덤한 피자명 또는 피자 가게 명을 반환합니다
    private fun randomValueFromList(list: List<String>): String = list.random();

    // 랜덤한 피자 메시지를 생성하고, 피자가게 명을 키로 나머지 정보를 값으로 하여 Hashmap을 생성 및 반환합니다.
    fun produceMessage(faker: Faker, id:Int): HashMap<String, String> {
        val shopId = randomValueFromList(pizzaShop);
        val pizzaName = randomValueFromList(pizzaNames);

        val ordId = "ord${id}"
        val customerName = faker.name().fullName()
        val phoneNumber = faker.phoneNumber().phoneNumber()
        val address = faker.address().streetAddress()
        val now = LocalDateTime.now()
        val message = " order_id: $ordId shop_id: $shopId pizza_name: $pizzaName customer_name: $customerName phone_number: $phoneNumber address: $address time: ${now.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss", Locale.KOREAN))}"

        val messageMap = HashMap<String, String>();

        messageMap["key"] = shopId
        messageMap["message"] = message

        return messageMap
    }
}

fun main(){
    val pizzaMessage = PizzaMessage()
    val seed = 2022L
    val random = Random(seed)
    val faker = Faker.instance(random)

    for (i in 1..59){
      val message = pizzaMessage.produceMessage(faker, i)
      println("key: ${message["key"]}, message: ${message["message"]}")
    }
}