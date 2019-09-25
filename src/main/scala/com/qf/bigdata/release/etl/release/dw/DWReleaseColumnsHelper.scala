package com.qf.bigdata.release.etl.release.dw

import com.qf.bigdata.release.enums.ReleaseStatusEnum

import scala.collection.mutable.ArrayBuffer

/**
  * DW层 投放业务列表
  */
object DWReleaseColumnsHelper {
  /**
    * 目标用户
    */
  def selectDWReleaseCustomerColumns(): ArrayBuffer[String] = {
    val columns = new ArrayBuffer[String]()
    columns.+=("release_session")
    columns.+=("release_status")
    columns.+=("device_num")
    columns.+=("device_type")
    columns.+=("sources")
    columns.+=("channels")
    columns.+=("get_json_object(exts,'$.idcard') as idcard")
    columns.+=("year(from_unixtime(unix_timestamp()))-substr(get_json_object(exts,'$.idcard'),7,4) as age")
    columns.+=("if(substr(get_json_object(exts,'$.idcard'),17,1)%2==0.0,'女','男') as gender")
    columns.+=("get_json_object(exts,'$.area_code') as area_code")
    columns.+=("get_json_object(exts,'$.longitude') as longitude")
    columns.+=("get_json_object(exts,'$.latitude') as latitude")
    columns.+=("get_json_object(exts,'$.matter_id') as matter_id")
    columns.+=("get_json_object(exts,'$.model_code') as model_code")
    columns.+=("get_json_object(exts,'$.model_version') as model_version")
    columns.+=("get_json_object(exts,'$.aid') as aid")
    columns.+=("ct")
    columns.+=("bdp_day")
    columns
  }

  /**
    * 曝光主题
    */
  def selectDWReleaseExposureColumns(): ArrayBuffer[String] = {
    val columns = new ArrayBuffer[String]()
    columns.+=("release_session")
    columns.+=("release_status")
    columns.+=("device_num")
    columns.+=("device_type")
    columns.+=("sources")
    columns.+=("channels")
    columns.+=("ct")
    columns.+=("bdp_day")
    columns
  }

  /**
    * 注册主题
    */
  def selectDWReleaseRegisterColumns(): ArrayBuffer[String] = {
    val columns = new ArrayBuffer[String]()
    columns.+=("get_json_object(exts,'$.user_register') as user_id")
    columns.+=("release_session")
    columns.+=("release_status")
    columns.+=("device_num")
    columns.+=("device_type")
    columns.+=("sources")
    columns.+=("channels")
    columns.+=("ct")
    columns.+=("bdp_day")
    columns
  }
  /**
    * 点击主题
    */
  def selectDWReleaseCheckColumns(): ArrayBuffer[String] = {
    val columns = new ArrayBuffer[String]()
    columns.+=("release_session")
    columns.+=("device_num")
    columns.+=("device_type")
    columns.+=("sources")
    columns.+=("channels")
    columns.+=("ct")
    columns.+=("bdp_day")
    columns.+=("release_status")
    columns
  }
  def selectDWReleaseColumns(status:String): ArrayBuffer[String] = {
    if(status.equals(ReleaseStatusEnum.CUSTOMER.getCode)){
      selectDWReleaseCustomerColumns
    }else  if(status.equals(ReleaseStatusEnum.SHOW.getCode)){
      selectDWReleaseExposureColumns()
    }else  if(status.equals(ReleaseStatusEnum.REGISTER.getCode)){
      selectDWReleaseRegisterColumns()
    }else  if(status.equals(ReleaseStatusEnum.CLICK.getCode)){
      selectDWReleaseCheckColumns()
    }else{
      new ArrayBuffer[String]()
    }
  }
}
