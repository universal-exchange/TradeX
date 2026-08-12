
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
# 1、演示 tradex_trader_future_uff 交易接口插件的使用；
# 2、演示 SubscribeInfo 和 UnsubscribeInfo 回调信息订阅退订；
# 3、演示 SubscribeData 和 UnsubscribeData 回调数据订阅退订；
# 4、演示 接口启动、接口停止、委托下单、委托撤单、资金查询、持仓查询等功能调用；

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

func_future_o_user_login    = 2101 # 期货用户登录
func_future_o_user_logout   = 2102 # 期货用户登出
func_future_t_single_order  = 2201 # 单个期货委托下单
func_future_t_single_cancel = 2202 # 单个期货委托撤单
func_future_q_user_capital  = 2302 # 查询客户资金
func_future_q_user_position = 2304 # 查询客户持仓
func_future_r_order         = 2901 # 报单回报
func_future_r_trans         = 2902 # 成交回报

def_trade_task_status_fail = -1 # 执行失败
def_trade_task_status_wait =  0 # 等待执行
def_trade_task_status_exec =  1 # 正在执行
def_trade_task_status_over =  2 # 执行完成

def_trade_task_query_type_easy = 1 # 简易查询
def_trade_task_query_type_full = 2 # 详细查询

def_trade_entr_type_l = 1 # 限价 limit
def_trade_entr_type_m = 2 # 市价 market
def_trade_exch_side_b = 1 # 买入 buy
def_trade_exch_side_s = 2 # 卖出 sell
def_trade_offset_o    = 1 # 开仓 open
def_trade_offset_c    = 2 # 平仓 close
def_trade_hedge_s     = 1 # 投机 speculation
def_trade_hedge_a     = 2 # 套利 Arbitrage
def_trade_hedge_h     = 3 # 套保 Hedge

g_caller_id = 0 # 递增
g_caller_wait_time = 5 # 秒

g_event_call_finish = threading.Event()

g_order = None
g_query = None
g_session = 0
g_strategy = 'future_uff'
g_caller_dict = {}
g_caller_execute_success = False

class deploy_future_uff(object):
    def __init__(self):
        self.flag = "future_uff"
        self.addr = "10.0.7.200"
        self.port = 8021
        self.trader = "tradex_trader_future_uff"

class config_trade(object):
    def __init__(self, deploy):
        self.trade_flag = deploy.flag
        self.trade_addr = deploy.addr
        self.trade_port = deploy.port
        self.trade_trader = deploy.trader
        self.trade_session = 0 # 在获得会话编号后赋值
    
    def ToJson(self):
        return json.dumps(self.__dict__)
        #return json.dumps(self.__dict__, sort_keys = False, indent = 4, separators = (",", ": "))

class caller_item(object):
    def __init__(self, caller_id):
        self.caller_id = caller_id # 任务编号
        self.return_code = 0 # 任务结果
        self.return_info = "" # 任务信息
        self.result_data = None # 任务数据
    
    def SetResult(self, result):
        self.return_code = result["return_code"]
        self.return_info = result["return_info"]
    
    def SetResultData(self, result):
        self.result_data = json.loads(result["result_data"])

class order_item(object):
    def __init__(self, strategy, session, instrument, exchange, entr_type, exch_side, offset, hedge, price, amount, order_flag = 0):
        self.order_id = "" # 委托编号
        self.get_order_id = False # 委托编号获取标识
        self.order_sys_id = "" # 报单编号
        self.get_order_sys_id = False # 报单编号获取标识
        self.instrument = instrument # 合约代码
        self.exchange = exchange # 交易所，CFFE:中金所，SHFE:上期所，CZCE:郑商所，DLCE:大商所，SGE:上海金交所
        self.entr_type = entr_type # 单个委托方式，1:限价，2:市价 # 组合委托方式，郑商所：8:跨期套利，9:跨品种套利，大商所：2:套利订单，7:互换订单
        self.exch_side = exch_side # 交易类型，1:买入，2:卖出，3:金属延期交割收货，4:金属延期交割交货，5:金属延期中立收货，6:金属延期中立交货
        self.offset = offset # 单个开平方向，1:开仓，2:平仓，3:强平，4:平今，5:平昨，6:强减，7:本地强平 # 组合开平方向，1:开仓，2:平仓 # 平今和平昨只对上期所和能源期货交易所有作用
        self.hedge = hedge # 投机套保，1:投机，2:套保，3:套利
        self.price = price # 委托价格
        self.amount = amount # 委托数量
        self.fill_qty = 0 # 成交数量
        self.finish_qty = 0 # 完成数量 # 仅供测试交易
        # 0：尚未申报，1：正在申报，2：非法委托，3：已报未成，4：部分成交，5：全部成交，6：等待撤单，7：部成部撤，8：全部撤单，9：撤单未成，10：等待修改，11：尚未触发，12：已经触发，13：自动挂起，14：未知状态
        self.status = 0 # 报单状态
        self.status_msg = "" # 状态信息 # 中文
        self.combin_flag = 0 # 组合标记 # 在 PlaceCombinOrder() 中置为 1
        self.trade_error = False # 交易异常标记
        self.order_flag = order_flag # 委托用户标识
        self.strategy = strategy # 用户策略标识
        self.session = session # 会话编号
        self.task_id = 0 # 任务编号
    
    def ToJson(self):
        return json.dumps(self.__dict__)
        #return json.dumps(self.__dict__, sort_keys = False, indent = 4, separators = (",", ": "))

class query_item(object):
    def __init__(self, strategy, session, order_id, instrument, exchange, query_type):
        self.order_id = order_id # 委托编号
        self.instrument = instrument # 合约代码
        self.exchange = exchange # 交易所，CFFE:中金所，SHFE:上期所，CZCE:郑商所，DLCE:大商所，SGE:上海金交所
        self.query_type = query_type # 查询方式，1：简易，2：详细
        self.strategy = strategy # 用户策略标识
        self.session = session # 会话编号
        self.task_id = 0 # 任务编号
    
    def ToJson(self):
        return json.dumps(self.__dict__)
        #return json.dumps(self.__dict__, sort_keys = False, indent = 4, separators = (",", ": "))

def HandleReturnData(result):
    try:
        call = result["call"]
        func = result["func"]
        form = result["form"]
        type = result["type"]
        if form == msg_code_json:
            data = json.loads(result["info"])
            if func == func_future_r_order:
                print("报单回报:", data["order_id"], data["order_sys_id"], data["instrument"], data["exchange"], 
                                  data["exch_side"], data["fill_qty"], data["status"], data["status_msg"], data["session"], data["strategy"])
            elif func == func_future_r_trans:
                print("成交回报:", data["order_id"], data["trans_id"], data["instrument"], data["exchange"], 
                                  data["exch_side"], data["fill_qty"], data["fill_price"], data["fill_time"], data["session"], data["strategy"])
            elif func == func_future_t_single_order:
                print("报单应答:", data["status"], data["finish"], data["message"], data["session"], data["strategy"])
            elif func == func_future_t_single_cancel:
                print("撤单应答:", data["status"], data["finish"], data["message"], data["session"], data["strategy"])
            elif func == func_future_q_user_capital:
                print("资金应答:")
                if len(data) > 0:
                    for capital in data:
                        print("account:", capital["account"], "currency:", capital["currency"], "available:", capital["available"], "margin:", capital["margin"], "frozen_margin:", capital["frozen_margin"])
                else:
                    print("无资金记录。")
            elif func == func_future_q_user_position:
                print("持仓应答:")
                if len(data) > 0:
                    for position in data:
                        print("instrument:", position["instrument"], "exch_side:", position["exch_side"], "position:", position["position"], "tod_position:", position["tod_position"], 
                              "pre_position:", position["pre_position"], "open_volume:", position["open_volume"], "close_volume:", position["close_volume"])
                else:
                    print("无持仓记录。")
    except Exception as e:
        print("HandleReturnData 异常！%s" % e)

def OnReturnInfo_01(result):
    try:
        if result["type"] == msg_func_return_data_xxx:
            HandleReturnData(result)
        elif result["type"] == msg_func_return_info_log:
            if result["form"] == msg_code_json:
                result = json.loads(result["info"])
                print("01", result["log_level"], result["log_cate"], result["log_info"])
    except Exception as e:
        print("OnReturnInfo_01 异常！%s" % e)

def OnReturnInfo_02(result):
    try:
        if result["type"] == msg_func_return_data_xxx:
            pass # 不在 02 中处理
        elif result["type"] == msg_func_return_info_log:
            if result["form"] == msg_code_json:
                result = json.loads(result["info"])
                print("02", result["log_level"], result["log_cate"], result["log_info"])
    except Exception as e:
        print("OnReturnInfo_02 异常！%s" % e)

def OnTraderStart(result):
    global g_session
    global g_caller_dict
    global g_caller_execute_success
    global g_event_call_finish
    try:
        caller_id = result["caller_id"]
        if caller_id in g_caller_dict.keys():
            caller_item = g_caller_dict[caller_id]
            caller_item.SetResult(result)
            if caller_item.return_code != 0:
                g_caller_execute_success = False
                print(caller_item.return_code, caller_item.return_info)
            else:
                g_caller_execute_success = True
                caller_item.SetResultData(result)
                g_session = result["session"] #
                print("TraderStart:", caller_item.return_info, caller_item.result_data, g_session)
        else:
            print("OnTraderStart 调用编号 缺失！%d" % caller_id)
    except Exception as e:
        print("OnTraderStart 异常！%s" % e)
    g_event_call_finish.set() #

def OnTraderStop(result):
    global g_caller_dict
    global g_caller_execute_success
    global g_event_call_finish
    try:
        caller_id = result["caller_id"]
        if caller_id in g_caller_dict.keys():
            caller_item = g_caller_dict[caller_id]
            caller_item.SetResult(result)
            if caller_item.return_code != 0:
                g_caller_execute_success = False
                print(caller_item.return_code, caller_item.return_info)
            else:
                g_caller_execute_success = True
                caller_item.SetResultData(result)
                print("TraderStop:", caller_item.return_info, caller_item.result_data)
        else:
            print("OnTraderStop 调用编号 缺失！%d" % caller_id)
    except Exception as e:
        print("OnTraderStop 异常！%s" % e)
    g_event_call_finish.set() #

def OnPlaceOrder(result):
    global g_order
    global g_caller_dict
    global g_caller_execute_success
    global g_event_call_finish
    try:
        caller_id = result["caller_id"]
        if caller_id in g_caller_dict.keys():
            caller_item = g_caller_dict[caller_id]
            caller_item.SetResult(result)
            if caller_item.return_code != 0:
                g_caller_execute_success = False
                print(caller_item.return_code, caller_item.return_info)
            else:
                g_caller_execute_success = True
                caller_item.SetResultData(result)
                g_order.task_id = caller_item.result_data["task_id"] #
                print("PlaceOrder:", caller_item.return_info, caller_item.result_data)
        else:
            print("OnPlaceOrder 调用编号 缺失！%d" % caller_id)
    except Exception as e:
        print("OnPlaceOrder 异常！%s" % e)
    g_event_call_finish.set() #

def OnCancelOrder(result):
    global g_caller_dict
    global g_caller_execute_success
    global g_event_call_finish
    try:
        caller_id = result["caller_id"]
        if caller_id in g_caller_dict.keys():
            caller_item = g_caller_dict[caller_id]
            caller_item.SetResult(result)
            if caller_item.return_code != 0:
                g_caller_execute_success = False
                print(caller_item.return_code, caller_item.return_info)
            else:
                g_caller_execute_success = True
                caller_item.SetResultData(result)
                print("CancelOrder:", caller_item.return_info, caller_item.result_data)
        else:
            print("OnCancelOrder 调用编号 缺失！%d" % caller_id)
    except Exception as e:
        print("OnCancelOrder 异常！%s" % e)
    g_event_call_finish.set() #

def OnQueryCapital(result):
    global g_caller_dict
    global g_caller_execute_success
    global g_event_call_finish
    try:
        caller_id = result["caller_id"]
        if caller_id in g_caller_dict.keys():
            caller_item = g_caller_dict[caller_id]
            caller_item.SetResult(result)
            if caller_item.return_code != 0:
                g_caller_execute_success = False
                print(caller_item.return_code, caller_item.return_info)
            else:
                g_caller_execute_success = True
                caller_item.SetResultData(result)
                print("QueryCapital:", caller_item.return_info, caller_item.result_data)
        else:
            print("OnQueryCapital 调用编号 缺失！%d" % caller_id)
    except Exception as e:
        print("OnQueryCapital 异常！%s" % e)
    g_event_call_finish.set() #

def OnQueryPosition(result):
    global g_caller_dict
    global g_caller_execute_success
    global g_event_call_finish
    try:
        caller_id = result["caller_id"]
        if caller_id in g_caller_dict.keys():
            caller_item = g_caller_dict[caller_id]
            caller_item.SetResult(result)
            if caller_item.return_code != 0:
                g_caller_execute_success = False
                print(caller_item.return_code, caller_item.return_info)
            else:
                g_caller_execute_success = True
                caller_item.SetResultData(result)
                print("QueryPosition:", caller_item.return_info, caller_item.result_data)
        else:
            print("OnQueryPosition 调用编号 缺失！%d" % caller_id)
    except Exception as e:
        print("OnQueryPosition 异常！%s" % e)
    g_event_call_finish.set() #

def TraderStart(module, config, callback):
    global g_caller_id
    global g_caller_dict
    global g_caller_wait_time
    global g_caller_execute_success
    global g_event_call_finish
    g_caller_id += 1
    g_event_call_finish.clear()
    g_caller_execute_success = False
    g_caller_dict[g_caller_id] = caller_item(g_caller_id)
    result = json.loads(module.DirectCall(g_caller_id, func_future_o_user_login, 0, config.ToJson(), callback)) # 异步
    print(result["return_code"], result["return_info"], result["caller_id"])
    if result["return_code"] != 0:
        return False
    else:
        caller_id = result["caller_id"]
        ret_wait = g_event_call_finish.wait(timeout = g_caller_wait_time) # 等待调用结果
        if ret_wait != True:
            print("等待 交易启用 结果超时！", caller_id)
            return False
        if g_caller_execute_success == False:
            print("交易启用 失败！", caller_id)
            return False
        else:
            print("交易启用 成功。", caller_id)
            return True
    return True

def TraderStop(module, config, callback):
    global g_caller_id
    global g_caller_dict
    global g_caller_wait_time
    global g_caller_execute_success
    global g_event_call_finish
    g_caller_id += 1
    g_event_call_finish.clear()
    g_caller_execute_success = False
    g_caller_dict[g_caller_id] = caller_item(g_caller_id)
    result = json.loads(module.DirectCall(g_caller_id, func_future_o_user_logout, 0, config.ToJson(), callback)) # 异步
    print(result["return_code"], result["return_info"], result["caller_id"])
    if result["return_code"] != 0:
        return False
    else:
        caller_id = result["caller_id"]
        ret_wait = g_event_call_finish.wait(timeout = g_caller_wait_time) # 等待调用结果
        if ret_wait != True:
            print("等待 交易停用 结果超时！", caller_id)
            return False
        if g_caller_execute_success == False:
            print("交易停用 失败！", caller_id)
            return False
        else:
            print("交易停用 成功。", caller_id)
            return True
    return True

def PlaceOrder(module, order, callback):
    global g_caller_id
    global g_caller_dict
    global g_caller_wait_time
    global g_caller_execute_success
    global g_event_call_finish
    g_caller_id += 1
    g_event_call_finish.clear()
    g_caller_execute_success = False
    g_caller_dict[g_caller_id] = caller_item(g_caller_id)
    result = json.loads(module.DirectCall(g_caller_id, func_future_t_single_order, 0, order.ToJson(), callback)) # 异步
    print(result["return_code"], result["return_info"], result["caller_id"])
    if result["return_code"] != 0:
        return False
    else:
        caller_id = result["caller_id"]
        ret_wait = g_event_call_finish.wait(timeout = g_caller_wait_time) # 等待调用结果
        if ret_wait != True:
            print("等待 委托下单 结果超时！", caller_id)
            return False
        if g_caller_execute_success == False:
            print("委托下单 失败！", caller_id)
            return False
        else:
            print("委托下单 成功。", caller_id)
            return True
    return True

def CancelOrder(module, order, callback):
    global g_caller_id
    global g_caller_dict
    global g_caller_wait_time
    global g_caller_execute_success
    global g_event_call_finish
    g_caller_id += 1
    g_event_call_finish.clear()
    g_caller_execute_success = False
    g_caller_dict[g_caller_id] = caller_item(g_caller_id)
    result = json.loads(module.DirectCall(g_caller_id, func_future_t_single_cancel, 0, order.ToJson(), callback)) # 异步
    print(result["return_code"], result["return_info"], result["caller_id"])
    if result["return_code"] != 0:
        return False
    else:
        caller_id = result["caller_id"]
        ret_wait = g_event_call_finish.wait(timeout = g_caller_wait_time) # 等待调用结果
        if ret_wait != True:
            print("等待 委托撤单 结果超时！", caller_id)
            return False
        if g_caller_execute_success == False:
            print("委托撤单 失败！", caller_id)
            return False
        else:
            print("委托撤单 成功。", caller_id)
            return True
    return True

def QueryCapital(module, query, callback):
    global g_strategy
    global g_caller_id
    global g_caller_dict
    global g_caller_wait_time
    global g_caller_execute_success
    global g_event_call_finish
    g_caller_id += 1
    g_event_call_finish.clear()
    g_caller_execute_success = False
    g_caller_dict[g_caller_id] = caller_item(g_caller_id)
    result = json.loads(module.DirectCall(g_caller_id, func_future_q_user_capital, 0, query.ToJson(), callback)) # 异步
    print(result["return_code"], result["return_info"], result["caller_id"])
    if result["return_code"] != 0:
        return False
    else:
        caller_id = result["caller_id"]
        ret_wait = g_event_call_finish.wait(timeout = g_caller_wait_time) # 等待调用结果
        if ret_wait != True:
            print("等待 资金查询 结果超时！", caller_id)
            return False
        if g_caller_execute_success == False:
            print("资金查询 失败！", caller_id)
            return False
        else:
            print("资金查询 成功。", caller_id)
            return True
    return True

def QueryPosition(module, query, callback):
    global g_strategy
    global g_caller_id
    global g_caller_dict
    global g_caller_wait_time
    global g_caller_execute_success
    global g_event_call_finish
    g_caller_id += 1
    g_event_call_finish.clear()
    g_caller_execute_success = False
    g_caller_dict[g_caller_id] = caller_item(g_caller_id)
    result = json.loads(module.DirectCall(g_caller_id, func_future_q_user_position, 0, query.ToJson(), callback)) # 异步
    print(result["return_code"], result["return_info"], result["caller_id"])
    if result["return_code"] != 0:
        return False
    else:
        caller_id = result["caller_id"]
        ret_wait = g_event_call_finish.wait(timeout = g_caller_wait_time) # 等待调用结果
        if ret_wait != True:
            print("等待 持仓查询 结果超时！", caller_id)
            return False
        if g_caller_execute_success == False:
            print("持仓查询 失败！", caller_id)
            return False
        else:
            print("持仓查询 成功。", caller_id)
            return True
    return True

def Test_TradeX_Trader_Future_UFF():
    global g_order
    global g_session
    global g_strategy
    global g_caller_wait_time
    global g_event_call_finish
    kernel = cyberx.Kernel(syscfg.SysCfg().ToArgs()) # 全局唯一
    module = cyberx.Create("tradex_trader_future_uff") # 全局唯一
    #module_01 = cyberx.Create("tradex_trader_future_uff") # 重复创建会报异常
    #module_01 = cyberx.GetCreate("tradex_trader_future_uff") # 可以获取已创建的实例
    
    subscribe_id_01 = module.SubscribeInfo(OnReturnInfo_01) # 订阅信息
    #subscribe_id_02 = module.SubscribeInfo(OnReturnInfo_02) # 订阅信息
    
    deploy = deploy_future_uff()
    
    config = config_trade(deploy)
    
    result = TraderStart(module, config, OnTraderStart)
    print(result)
    
    if False:
        g_order = order_item(g_strategy, g_session, "IF2512", "CFFE", def_trade_entr_type_l, def_trade_exch_side_b, def_trade_offset_o, def_trade_hedge_s, 4620.0, 1)
        result = PlaceOrder(module, g_order, OnPlaceOrder)
        print(result)
        
        g_event_call_finish.clear()
        g_event_call_finish.wait(timeout = g_caller_wait_time) # 秒
        
        print("g_order.task_id:", g_order.task_id)
        
        result = CancelOrder(module, g_order, OnCancelOrder)
        print(result)
        
        g_event_call_finish.clear()
        g_event_call_finish.wait(timeout = g_caller_wait_time) # 秒
    
    if False:
        g_query = query_item(g_strategy, g_session, "", "", "", def_trade_task_query_type_easy)
        result = QueryCapital(module, g_query, OnQueryCapital)
        print(result)
        
        g_event_call_finish.clear()
        g_event_call_finish.wait(timeout = g_caller_wait_time) # 秒
    
    if True:
        g_query = query_item(g_strategy, g_session, "", "", "", def_trade_task_query_type_easy)
        result = QueryPosition(module, g_query, OnQueryPosition)
        print(result)
        
        g_event_call_finish.clear()
        g_event_call_finish.wait(timeout = g_caller_wait_time) # 秒
    
    config.trade_session = g_session #
    result = TraderStop(module, config, OnTraderStop)
    print(result)
    
    g_event_call_finish.clear()
    g_event_call_finish.wait(timeout = g_caller_wait_time) # 秒
    
    module.UnsubscribeInfo(subscribe_id_01) # 退订信息
    #module.UnsubscribeInfo(subscribe_id_02) # 退订信息

if __name__ == "__main__":
    Test_TradeX_Trader_Future_UFF()
