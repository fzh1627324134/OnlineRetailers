package com.daoke360.task.session


case class CateGorySecondSort(click_count: Int, cart_count: Int, order_count: Int, pay_count: Int) extends Ordered[CateGorySecondSort] with Serializable{
  override def compare(that: CateGorySecondSort): Int = {
    if (this.click_count - that.click_count != 0){
      this.click_count -that.click_count
    }else if (this.cart_count-that.cart_count!=0){
      this.cart_count-that.cart_count
    }else if (this.order_count-that.order_count!=0){
      this.order_count-that.order_count
    }else{
      this.pay_count-that.pay_count
    }
  }

  override def toString: String = s"CategorySecondSort(click_count=$click_count,cart_count=$cart_count,order_count=$order_count,pay_count=$pay_count)"

}
