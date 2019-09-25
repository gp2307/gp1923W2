package com.qf.bigdata.release.constant

import com.qf.bigdata.release.enums.ReleaseStatusEnum
import org.apache.spark.storage.StorageLevel

/**
  * 常量
  */
object ReleaseConstant {
  //partition
  val DEF_STORAGE_LEVEL = StorageLevel.MEMORY_AND_DISK
  val DEF_PARTITION:String = "bdp_day"
  val DEF_SOURCE_PARTITION = 4
  //维度列
  val COL_RELEASE_SESSION_STATUS:String = "release_status"

  //ods=====================================================
  val ODS_RELEASE_SESSION = "ods_release.ods_01_release_session"

  //dw======================================================
  val DW_RELEASE_CUSTOMER = "dw_release.dw_release_customer"
  val DW_RELEASE_EXPOSURE = "dw_release.dw_release_exposure"
  val DW_RELEASE_REGISTER = "dw_release.dw_release_register_users"
  val DW_RELEASE_CHICK = "dw_release.dw_release_click"
  val DW_RELEASE_STATUS:Map[String,String] = Map((ReleaseStatusEnum.CUSTOMER.getCode,DW_RELEASE_CUSTOMER),
    (ReleaseStatusEnum.SHOW.getCode,DW_RELEASE_EXPOSURE),
    (ReleaseStatusEnum.REGISTER.getCode,DW_RELEASE_REGISTER),
    (ReleaseStatusEnum.CLICK.getCode,DW_RELEASE_CHICK))
}
