package com.westar.dataset

case class Dog(name: String, color: String)

case class Person(name: String, age: Long)

case class TrackerSession(session_id: String, session_server_time: String,
                          cookie: String, cookie_label: String, ip: String, landing_url: String,
                          pageview_count: Int, click_count: Int, domain: String, domain_label: String)

case class User(userId: String, userName: String, age: Long)
case class Order(id: String, userId: String, userName: String, totalPrice: Double)


case class TestData(key: Int, value: String)
case class TestData2(a: Int, b: Int)