package com.jwszol

/**
  * Created by jwszol on 12/06/17.
  */
case class Transactions (
  trans_id: String,
  prod_id: String,
  user_id: String,
  purchase_amount: String,
  item_desc: String
  )

case class Users (
  new_id: Option[String],
  email: String,
  location: String
  )



