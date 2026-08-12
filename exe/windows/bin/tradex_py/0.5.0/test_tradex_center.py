
# -*- coding: utf-8 -*-

# Copyright (c) 2025-2025 the DerivX authors
# All rights reserved.
#
# The project sponsor and lead author is Xu Rendong.
# E-mail: xrd@ustc.edu, QQ: 277195007, WeChat: xrd_ustc
# See the contributors file for names of other contributors.
#
# Commercial use of this code in source and binary forms is
# governed by a LGPL v3 license. You may get a copy from the
# root directory. Or else you should get a specific written 
# permission from the project author.
#
# Individual and educational use of this code in source and
# binary forms is governed by a 3-clause BSD license. You may
# get a copy from the root directory. Certainly welcome you
# to contribute code of all sorts.
#
# Be sure to retain the above copyright notice and conditions.

# 示例说明：
# 1、演示 tradex_center 交易中心父级插件的使用；
# 2、依赖交易接口子级插件：tradex_client_future_uff、tradex_client_stock_ufs；
# 3、演示 DirectCall 的同步和异步调用；
# 4、演示 SubscribeInfo 和 UnsubscribeInfo 回调信息订阅退订；

# 注意：版本 >= 0.5.14 的，编译环境 Visual Studio 从 17.9.X 升级为 17.10.X 后，
#      对于 Python 3.6、3.7、3.8、3.9、3.10、3.11 存在一些兼容问题，
#      需要将 import cyberx 语句放在 import 如 pandas、PyQt5 等其他第三方库之前，
#      对于 Python 3.12 则仍然可以正常地任意放置，初始化 cyberx.Kernel 时不会异常。

import json
import threading

import cyberx

import syscfg
# import cyberx

#msg_code_string   = 1 # 直接字符串
msg_code_json     = 2 # Json格式
#msg_code_base64   = 3 # Base64格式
#msg_code_protobuf = 4 # ProtoBuf格式
#msg_code_zlib     = 5 # ZLib格式
#msg_code_msgpack  = 6 # MsgPack格式

msg_func_return_info_log = 1 # 回调返回的日志信息
msg_func_return_data_xxx = 2 # 回调返回的某类数据

func_client_start   = 1
func_client_stop    = 2
func_get_trade_data = 3

func_trade_data_test = 0 # 调试

make_call_id = 0 # 递增
call_wait_time = 10 # 秒
success_client_start_stop = False

event_call_finish = threading.Event()

class deploy_stock_ufs(object):
    def __init__(self):
        self.flag = "stock_ufs"
        self.addr = "10.0.7.200"
        self.port = 8011
        self.client = "tradex_client_stock_ufs"

class deploy_future_uff(object):
    def __init__(self):
        self.flag = "future_uff"
        self.addr = "10.0.7.200"
        self.port = 8021
        self.client = "tradex_client_future_uff"

class config_trade(object):
    def __init__(self, deploy):
        self.trade_flag = deploy.flag
        self.trade_addr = deploy.addr
        self.trade_port = deploy.port
        self.trade_client = deploy.client
    
    def ToJson(self):
        return json.dumps(self.__dict__)
        #return json.dumps(self.__dict__, sort_keys = False, indent = 4, separators = (",", ": "))

def OnClientStart(result):
    global success_client_start_stop
    try:
        if result["return_code"] != 0:
            success_client_start_stop = False
            print(result["return_code"], result["return_info"])
        else:
            success_client_start_stop = True
            result_data = json.loads(result["result_data"])
            print("ClientStart:", result["return_info"], result_data)
    except Exception as e:
        print("OnClientStart 异常！%s" % e)
    event_call_finish.set() #

def OnClientStop(result):
    global success_client_start_stop
    try:
        if result["return_code"] != 0:
            success_client_start_stop = False
            print(result["return_code"], result["return_info"])
        else:
            success_client_start_stop = True
            result_data = json.loads(result["result_data"])
            print("ClientStop:", result["return_info"], result_data)
    except Exception as e:
        print("OnClientStop 异常！%s" % e)
    event_call_finish.set() #

def ClientStart(module, config, callback):
    global make_call_id
    global success_client_start_stop
    make_call_id += 1
    event_call_finish.clear()
    success_client_start_stop = False
    result = json.loads(module.DirectCall(make_call_id, func_client_start, 0, config.ToJson(), callback)) # 异步
    print(result["return_code"], result["return_info"], result["caller_id"])
    if result["return_code"] != 0:
        return False
    else:
        caller_id = result["caller_id"]
        ret_wait = event_call_finish.wait(timeout = call_wait_time) # 等待调用结果
        if ret_wait != True:
            print("等待 交易启用 结果超时！", caller_id)
            return False
        if success_client_start_stop == False:
            print("交易启用 失败！", caller_id)
            return False
        else:
            print("交易启用 成功。", caller_id)
            return True
    return True

def ClientStop(module, config, callback):
    global make_call_id
    global success_client_start_stop
    make_call_id += 1
    event_call_finish.clear()
    success_client_start_stop = False
    result = json.loads(module.DirectCall(make_call_id, func_client_stop, 0, config.ToJson(), callback)) # 异步
    print(result["return_code"], result["return_info"], result["caller_id"])
    if result["return_code"] != 0:
        return False
    else:
        caller_id = result["caller_id"]
        ret_wait = event_call_finish.wait(timeout = call_wait_time) # 等待调用结果
        if ret_wait != True:
            print("等待 交易停用 结果超时！", caller_id)
            return False
        if success_client_start_stop == False:
            print("交易停用 失败！", caller_id)
            return False
        else:
            print("交易停用 成功。", caller_id)
            return True
    return True

def OnReturnInfo_01(result):
    try:
        if result["type"] == msg_func_return_info_log:
            if result["form"] == msg_code_json:
                result = json.loads(result["info"])
                print("01", result["log_level"], result["log_cate"], result["log_info"])
    except Exception as e:
        print("OnReturnInfo_01 异常！%s" % e)

def OnReturnInfo_02(result):
    try:
        if result["type"] == msg_func_return_info_log:
            if result["form"] == msg_code_json:
                result = json.loads(result["info"])
                print("02", result["log_level"], result["log_cate"], result["log_info"])
    except Exception as e:
        print("OnReturnInfo_02 异常！%s" % e)

def GetTradeData_Test(module):
    for i in range(call_wait_time):
        config_get_trade_data = {"trade_type":func_trade_data_test, "trade_exchange":"CFFEX"}
        result = json.loads(module.DirectCall(0, func_get_trade_data, 0, json.dumps(config_get_trade_data))) # 同步
        if result["return_code"] != 0:
            print(result["return_code"], result["return_info"])
        else:
            trade_data = result["result_data"]
            print(trade_data["trade_type"], trade_data["trade_exchange"])
        event_call_finish.clear()
        event_call_finish.wait(timeout = 1) # 秒

def Test_TradeX_Center():
    kernel = cyberx.Kernel(syscfg.SysCfg().ToArgs()) # 全局唯一
    module = cyberx.Create("tradex_center") # 全局唯一
    #module_01 = cyberx.Create("tradex_center") # 重复创建会报异常
    #module_01 = cyberx.GetCreate("tradex_center") # 可以获取已创建的实例
    subscribe_id_01 = module.SubscribeInfo(OnReturnInfo_01) # 订阅信息
    subscribe_id_02 = module.SubscribeInfo(OnReturnInfo_02) # 订阅信息
    
    deploy = deploy_stock_ufs()
    #deploy = deploy_future_uff()
    
    config = config_trade(deploy)
    
    result = ClientStart(module, config, OnClientStart)
    print(result)
    
    GetTradeData_Test(module)
    
    #event_call_finish.clear()
    #event_call_finish.wait(timeout = call_wait_time) # 秒
    
    result = ClientStop(module, config, OnClientStop)
    print(result)
    
    event_call_finish.clear()
    event_call_finish.wait(timeout = call_wait_time) # 秒
    
    module.UnsubscribeInfo(subscribe_id_01) # 退订信息
    module.UnsubscribeInfo(subscribe_id_02) # 退订信息

if __name__ == "__main__":
    Test_TradeX_Center()
