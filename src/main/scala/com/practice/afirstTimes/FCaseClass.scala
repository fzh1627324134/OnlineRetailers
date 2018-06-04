package com.practice.afirstTimes

case class FIPRule(var startIP:Long,var endIP:Long,var country:String,var province:String,var city:String) extends Serializable

case class FRegionInfo(var country:String = FGlobalContants.DEFAULT_VALUE,var province:String = FGlobalContants.DEFAULT_VALUE,var city:String = FGlobalContants.DEFAULT_VALUE) extends Serializable
