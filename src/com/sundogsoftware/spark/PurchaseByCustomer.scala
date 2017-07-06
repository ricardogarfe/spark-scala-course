package com.sundogsoftware.spark

import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkContext

object PurchaseByCustomer {
  /** A function that splits a line of input into (age, numFriends) tuples. */
  def parseLine(line: String) = {
    // Split by commas
    val fields = line.split(",")
    (fields(0).toInt, fields(2).toFloat)
  }

  /** Our main function where the action happens */
  def main(args: Array[String]) {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "PurchaseByCustomer")

    // Load each line of the source data into an RDD
    val lines = sc.textFile("../customer-orders.csv")

    // Use our parseLines function to convert to (age, numFriends) tuples
    val rdd = lines.map(parseLine)

    var customerPurchases = rdd.reduceByKey((x, y) => x + y)

    var orderByPurchases = customerPurchases.map(x => (x._2, x._1))

    var sortedPurchasesByAmount = orderByPurchases.sortByKey()
    
    for (result <- sortedPurchasesByAmount.collect()) {
      val user = result._2
      val purchased = result._1
      println(s"User $user purchased: $purchased")
    }

  }
}