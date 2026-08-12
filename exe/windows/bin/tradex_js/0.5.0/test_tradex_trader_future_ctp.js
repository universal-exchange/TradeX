/*
* Copyright (c) 2025-2025 the DerivX authors
* All rights reserved.
*
* The project sponsor and lead author is Xu Rendong.
* E-mail: xrd@ustc.edu, QQ: 277195007, WeChat: xrd_ustc
* See the contributors file for names of other contributors.
*
* Commercial use of this code in source and binary forms is
* governed by a LGPL v3 license. You may get a copy from the
* root directory. Or else you should get a specific written
* permission from the project author.
*
* Individual and educational use of this code in source and
* binary forms is governed by a 3-clause BSD license. You may
* get a copy from the root directory. Certainly welcome you
* to contribute code of all sorts.
*
* Be sure to retain the above copyright notice and conditions.
*/

// 示例说明：
// 1、演示 tradex_trader_future_ctp 交易接口插件的使用；
// 2、演示 SubscribeInfo 和 UnsubscribeInfo 回调信息订阅退订；
// 3、演示 SubscribeData 和 UnsubscribeData 回调数据订阅退订；
// 4、演示 接口启动、接口停止、委托下单、委托撤单、资金查询、持仓查询等功能调用；

'use strict'

// 使用 timers/promises 的 setTimeout 要求 Node 版本为 15.0.0 及以上
// 以后可以用 timers/promises 的 scheduler.wait 代替，要求 Node 版本为 17.3.0 及以上
const promises = require('timers/promises')

const syscfg = require('./syscfg')
const cyberx = require('cyberx') // cyberx-js

//let msg_code_string   = 1 // 直接字符串
let msg_code_json     = 2 // Json格式
//let msg_code_base64   = 3 // Base64格式
//let msg_code_protobuf = 4 // ProtoBuf格式
//let msg_code_zlib     = 5 // ZLib格式
//let msg_code_msgpack  = 6 // MsgPack格式

let msg_func_return_info_log = 1 // 回调返回的日志信息
let msg_func_return_data_xxx = 2 // 回调返回的某类数据

let func_future_o_user_login    = 2101 // 期货用户登录
let func_future_o_user_logout   = 2102 // 期货用户登出
let func_future_t_single_order  = 2201 // 单个期货委托下单
let func_future_t_single_cancel = 2202 // 单个期货委托撤单
let func_future_q_user_capital  = 2302 // 查询客户资金
let func_future_q_user_position = 2304 // 查询客户持仓
let func_future_r_order         = 2901 // 报单回报
let func_future_r_trans         = 2902 // 成交回报

let def_trade_task_status_fail = -1 // 执行失败
let def_trade_task_status_wait =  0 // 等待执行
let def_trade_task_status_exec =  1 // 正在执行
let def_trade_task_status_over =  2 // 执行完成

let def_trade_task_query_type_easy = 1 // 简易查询
let def_trade_task_query_type_full = 2 // 详细查询

let def_trade_entr_type_l = 1 // 限价 limit
let def_trade_entr_type_m = 2 // 市价 market
let def_trade_exch_side_b = 1 // 买入 buy
let def_trade_exch_side_s = 2 // 卖出 sell
let def_trade_offset_o    = 1 // 开仓 open
let def_trade_offset_c    = 2 // 平仓 close
let def_trade_hedge_s     = 1 // 投机 speculation
let def_trade_hedge_a     = 2 // 套利 Arbitrage
let def_trade_hedge_h     = 3 // 套保 Hedge

let g_caller_id = 0 // 递增
let g_caller_wait_time = 5 // 秒

let g_event_call_finish = null // AbortController

let g_order = null
let g_query = null
let g_session = 0
let g_strategy = 'future_ctp'
let g_caller_dict = new Map()
let g_caller_execute_success = false

class deploy_future_ctp {
    constructor() {
        this.flag = 'future_ctp'
        this.addr = '10.0.7.200'
        this.port = 8021
        this.trader = 'tradex_trader_future_ctp'
    }
}

class config_trade {
    constructor(deploy) {
        this.trade_flag = deploy.flag
        this.trade_addr = deploy.addr
        this.trade_port = deploy.port
        this.trade_trader = deploy.trader
        this.trade_session = 0 // 在获得会话编号后赋值
    }
    
    ToJson() {
        return JSON.stringify(this)
        //return JSON.stringify(this, null, 4)
    }
}

class caller_item {
    constructor(caller_id) {
        this.caller_id = caller_id // 任务编号
        this.return_code = 0 // 任务结果
        this.return_info = '' // 任务信息
        this.result_data = null // 任务数据
    }
    
    SetResult(result) {
        this.return_code = result['return_code']
        this.return_info = result['return_info']
    }
    
    SetResultData(result) {
        this.result_data = JSON.parse(result['result_data'])
    }
}

class order_item {
    constructor(strategy, session, instrument, exchange, entr_type, exch_side, offset, hedge, price, amount, order_flag = 0) {
        this.order_id = '' // 委托编号
        this.get_order_id = false // 委托编号获取标识
        this.order_sys_id = '' // 报单编号
        this.get_order_sys_id = false // 报单编号获取标识
        this.instrument = instrument // 合约代码
        this.exchange = exchange // 交易所，CFFE:中金所，SHFE:上期所，CZCE:郑商所，DLCE:大商所，SGE:上海金交所
        this.entr_type = entr_type // 单个委托方式，1:限价，2:市价 // 组合委托方式，郑商所：8:跨期套利，9:跨品种套利，大商所：2:套利订单，7:互换订单
        this.exch_side = exch_side // 交易类型，1:买入，2:卖出，3:金属延期交割收货，4:金属延期交割交货，5:金属延期中立收货，6:金属延期中立交货
        this.offset = offset // 单个开平方向，1:开仓，2:平仓，3:强平，4:平今，5:平昨，6:强减，7:本地强平 // 组合开平方向，1:开仓，2:平仓 // 平今和平昨只对上期所和能源期货交易所有作用
        this.hedge = hedge // 投机套保，1:投机，2:套保，3:套利
        this.price = price // 委托价格
        this.amount = amount // 委托数量
        this.fill_qty = 0 // 成交数量
        this.finish_qty = 0 // 完成数量 // 仅供测试交易
        // 0：尚未申报，1：正在申报，2：非法委托，3：已报未成，4：部分成交，5：全部成交，6：等待撤单，7：部成部撤，8：全部撤单，9：撤单未成，10：等待修改，11：尚未触发，12：已经触发，13：自动挂起，14：未知状态
        this.status = 0 // 报单状态
        this.status_msg = '' // 状态信息 // 中文
        this.combin_flag = 0 // 组合标记 // 在 PlaceCombinOrder() 中置为 1
        this.trade_error = false // 交易异常标记
        this.order_flag = order_flag // 委托用户标识
        this.strategy = strategy // 用户策略标识
        this.session = session // 会话编号
        this.task_id = 0 // 任务编号
    }
    
    ToJson() {
        return JSON.stringify(this)
        //return JSON.stringify(this, null, 4)
    }
}

class query_item {
    constructor(strategy, session, order_id, instrument, exchange, query_type) {
        this.order_id = order_id // 委托编号
        this.instrument = instrument // 合约代码
        this.exchange = exchange // 交易所，CFFE:中金所，SHFE:上期所，CZCE:郑商所，DLCE:大商所，SGE:上海金交所
        this.query_type = query_type // 查询方式，1：简易，2：详细
        this.strategy = strategy // 用户策略标识
        this.session = session // 会话编号
        this.task_id = 0 // 任务编号
    }
    
    ToJson() {
        return JSON.stringify(this)
        //return JSON.stringify(this, null, 4)
    }
}

function HandleReturnData(result) {
    try {
        let call = result['call']
        let func = result['func']
        let form = result['form']
        let type = result['type']
        if(form === msg_code_json) {
            let data = JSON.parse(result['info'])
            if(func === func_future_r_order) {
                console.log('报单回报:', data['order_id'], data['order_sys_id'], data['instrument'], data['exchange'], 
                                        data['exch_side'], data['fill_qty'], data['status'], data['status_msg'], data['session'], data['strategy'])
            }
            else if(func === func_future_r_trans) {
                console.log('成交回报:', data['order_id'], data['trans_id'], data['instrument'], data['exchange'], 
                                        data['exch_side'], data['fill_qty'], data['fill_price'], data['fill_time'], data['session'], data['strategy'])
            }
            else if(func === func_future_t_single_order) {
                console.log('报单应答:', data['status'], data['finish'], data['message'], data['session'], data['strategy'])
            }
            else if(func === func_future_t_single_cancel) {
                console.log('撤单应答:', data['status'], data['finish'], data['message'], data['session'], data['strategy'])
            }
            else if(func === func_future_q_user_capital) {
                console.log('资金应答:')
                if(data.length > 0) {
                    data.forEach(function(capital) {
                        console.log('account:', capital['account'], 'currency:', capital['currency'], 'available:', capital['available'], 'margin:', capital['margin'], 'frozen_margin:', capital['frozen_margin'])
                    })
                }
                else {
                    console.log('无资金记录。')
                }
            }
            else if(func === func_future_q_user_position) {
                console.log('持仓应答:')
                if(data.length > 0) {
                    data.forEach(function(position) {
                        console.log('instrument:', position['instrument'], 'exch_side:', position['exch_side'], 'position:', position['position'], 'tod_position:', position['tod_position'], 
                                    'pre_position:', position['pre_position'], 'open_volume:', position['open_volume'], 'close_volume:', position['close_volume'])
                    })
                }
                else {
                    console.log('无持仓记录。')
                }
            }
        }
    }
    catch(error) {
        console.log('HandleReturnData 异常！' + error)
    }
}

function OnReturnInfo_01() {
    try {
        let [result] = Array.from(arguments)
        if(result['type'] === msg_func_return_data_xxx) {
            HandleReturnData(result)
        }
        else if(result['type'] === msg_func_return_info_log) {
            if(result['form'] === msg_code_json) {
                result = JSON.parse(result['info'])
                console.log('01', result['log_level'], result['log_cate'], result['log_info'])
            }
        }
    }
    catch(error) {
        console.log('OnReturnInfo_01 异常！' + error)
    }
}

function OnReturnInfo_02() {
    try {
        let [result] = Array.from(arguments)
        if(result['type'] === msg_func_return_data_xxx) {
            // 不在 02 中处理
        }
        else if(result['type'] === msg_func_return_info_log) {
            if(result['form'] === msg_code_json) {
                result = JSON.parse(result['info'])
                console.log('02', result['log_level'], result['log_cate'], result['log_info'])
            }
        }
    }
    catch(error) {
        console.log('OnReturnInfo_02 异常！' + error)
    }
}

function OnTraderStart() {
    try {
        let [result] = Array.from(arguments)
        let caller_id = result['caller_id']
        if(g_caller_dict.has(caller_id)) {
            let caller_item = g_caller_dict.get(caller_id)
            caller_item.SetResult(result)
            if(caller_item.return_code !== 0) {
                g_caller_execute_success = false
                console.log(caller_item.return_code, caller_item.return_info)
            }
            else {
                g_caller_execute_success = true
                caller_item.SetResultData(result)
                g_session = result['session'] //
                console.log('TraderStart:', caller_item.return_info, caller_item.result_data, g_session)
            }
        }
        else {
            console.log('OnTraderStart 调用编号 缺失！' + caller_id)
        }
    }
    catch(error) {
        console.log('OnTraderStart 异常！' + error)
    }
    g_event_call_finish.abort() //
}

function OnTraderStop() {
    try {
        let [result] = Array.from(arguments)
        let caller_id = result['caller_id']
        if(g_caller_dict.has(caller_id)) {
            let caller_item = g_caller_dict.get(caller_id)
            caller_item.SetResult(result)
            if(caller_item.return_code !== 0) {
                g_caller_execute_success = false
                console.log(caller_item.return_code, caller_item.return_info)
            }
            else {
                g_caller_execute_success = true
                caller_item.SetResultData(result)
                console.log('TraderStop:', caller_item.return_info, caller_item.result_data)
            }
        }
        else {
            console.log('OnTraderStop 调用编号 缺失！' + caller_id)
        }
    }
    catch(error) {
        console.log('OnTraderStop 异常！' + error)
    }
    g_event_call_finish.abort() //
}

function OnPlaceOrder() {
    try {
        let [result] = Array.from(arguments)
        let caller_id = result['caller_id']
        if(g_caller_dict.has(caller_id)) {
            let caller_item = g_caller_dict.get(caller_id)
            caller_item.SetResult(result)
            if(caller_item.return_code !== 0) {
                g_caller_execute_success = false
                console.log(caller_item.return_code, caller_item.return_info)
            }
            else {
                g_caller_execute_success = true
                caller_item.SetResultData(result)
                g_order.task_id = caller_item.result_data["task_id"] //
                console.log('PlaceOrder:', caller_item.return_info, caller_item.result_data)
            }
        }
        else {
            console.log('OnPlaceOrder 调用编号 缺失！' + caller_id)
        }
    }
    catch(error) {
        console.log('OnPlaceOrder 异常！' + error)
    }
    g_event_call_finish.abort() //
}

function OnCancelOrder() {
    try {
        let [result] = Array.from(arguments)
        let caller_id = result['caller_id']
        if(g_caller_dict.has(caller_id)) {
            let caller_item = g_caller_dict.get(caller_id)
            caller_item.SetResult(result)
            if(caller_item.return_code !== 0) {
                g_caller_execute_success = false
                console.log(caller_item.return_code, caller_item.return_info)
            }
            else {
                g_caller_execute_success = true
                caller_item.SetResultData(result)
                console.log('CancelOrder:', caller_item.return_info, caller_item.result_data)
            }
        }
        else {
            console.log('OnCancelOrder 调用编号 缺失！' + caller_id)
        }
    }
    catch(error) {
        console.log('OnCancelOrder 异常！' + error)
    }
    g_event_call_finish.abort() //
}

function OnQueryCapital() {
    try {
        let [result] = Array.from(arguments)
        let caller_id = result['caller_id']
        if(g_caller_dict.has(caller_id)) {
            let caller_item = g_caller_dict.get(caller_id)
            caller_item.SetResult(result)
            if(caller_item.return_code !== 0) {
                g_caller_execute_success = false
                console.log(caller_item.return_code, caller_item.return_info)
            }
            else {
                g_caller_execute_success = true
                caller_item.SetResultData(result)
                console.log('QueryCapital:', caller_item.return_info, caller_item.result_data)
            }
        }
        else {
            console.log('OnQueryCapital 调用编号 缺失！' + caller_id)
        }
    }
    catch(error) {
        console.log('OnQueryCapital 异常！' + error)
    }
    g_event_call_finish.abort() //
}

function OnQueryPosition() {
    try {
        let [result] = Array.from(arguments)
        let caller_id = result['caller_id']
        if(g_caller_dict.has(caller_id)) {
            let caller_item = g_caller_dict.get(caller_id)
            caller_item.SetResult(result)
            if(caller_item.return_code !== 0) {
                g_caller_execute_success = false
                console.log(caller_item.return_code, caller_item.return_info)
            }
            else {
                g_caller_execute_success = true
                caller_item.SetResultData(result)
                console.log('QueryPosition:', caller_item.return_info, caller_item.result_data)
            }
        }
        else {
            console.log('OnQueryPosition 调用编号 缺失！' + caller_id)
        }
    }
    catch(error) {
        console.log('OnQueryPosition 异常！' + error)
    }
    g_event_call_finish.abort() //
}

async function TraderStart(module, config, callback) {
    g_caller_id += 1
    g_caller_execute_success = false
    g_event_call_finish = new AbortController()
    g_caller_dict.set(g_caller_id, new caller_item(g_caller_id))
    let result = JSON.parse(module.DirectCall(g_caller_id, func_future_o_user_login, 0, config.ToJson(), callback)) // 异步
    console.log(result['return_code'], result['return_info'], result['caller_id'])
    if(result['return_code'] !== 0) {
        return false
    }
    else {
        let caller_id = result['caller_id']
        const ret_wait = await promises.setTimeout(g_caller_wait_time * 1000, '', { signal:g_event_call_finish.signal }).then(() => false, err => true) // 等待调用结果
        if(ret_wait != true) {
            console.log('等待 交易启用 结果超时！' + caller_id)
            return false
        }
        if(g_caller_execute_success === false) {
            console.log('交易启用 失败！' + caller_id)
            return false
        }
        else {
            console.log('交易启用 成功。' + caller_id)
            return true
        }
    }
    return true
}

async function TraderStop(module, config, callback) {
    g_caller_id += 1
    g_caller_execute_success = false
    g_event_call_finish = new AbortController()
    g_caller_dict.set(g_caller_id, new caller_item(g_caller_id))
    let result = JSON.parse(module.DirectCall(g_caller_id, func_future_o_user_logout, 0, config.ToJson(), callback)) // 异步
    console.log(result['return_code'], result['return_info'], result['caller_id'])
    if(result['return_code'] !== 0) {
        return false
    }
    else {
        let caller_id = result['caller_id']
        const ret_wait = await promises.setTimeout(g_caller_wait_time * 1000, '', { signal:g_event_call_finish.signal }).then(() => false, err => true) // 等待调用结果
        if(ret_wait != true) {
            console.log('等待 交易停用 结果超时！' + caller_id)
            return false
        }
        if(g_caller_execute_success === false) {
            console.log('交易停用 失败！' + caller_id)
            return false
        }
        else {
            console.log('交易停用 成功。' + caller_id)
            return true
        }
    }
    return true
}

async function PlaceOrder(module, order, callback) {
    g_caller_id += 1
    g_caller_execute_success = false
    g_event_call_finish = new AbortController()
    g_caller_dict.set(g_caller_id, new caller_item(g_caller_id))
    let result = JSON.parse(module.DirectCall(g_caller_id, func_future_t_single_order, 0, order.ToJson(), callback)) // 异步
    console.log(result['return_code'], result['return_info'], result['caller_id'])
    if(result['return_code'] !== 0) {
        return false
    }
    else {
        let caller_id = result['caller_id']
        const ret_wait = await promises.setTimeout(g_caller_wait_time * 1000, '', { signal:g_event_call_finish.signal }).then(() => false, err => true) // 等待调用结果
        if(ret_wait != true) {
            console.log('等待 委托下单 结果超时！' + caller_id)
            return false
        }
        if(g_caller_execute_success === false) {
            console.log('委托下单 失败！' + caller_id)
            return false
        }
        else {
            console.log('委托下单 成功。' + caller_id)
            return true
        }
    }
    return true
}

async function CancelOrder(module, order, callback) {
    g_caller_id += 1
    g_caller_execute_success = false
    g_event_call_finish = new AbortController()
    g_caller_dict.set(g_caller_id, new caller_item(g_caller_id))
    let result = JSON.parse(module.DirectCall(g_caller_id, func_future_t_single_cancel, 0, order.ToJson(), callback)) // 异步
    console.log(result['return_code'], result['return_info'], result['caller_id'])
    if(result['return_code'] !== 0) {
        return false
    }
    else {
        let caller_id = result['caller_id']
        const ret_wait = await promises.setTimeout(g_caller_wait_time * 1000, '', { signal:g_event_call_finish.signal }).then(() => false, err => true) // 等待调用结果
        if(ret_wait != true) {
            console.log('等待 委托撤单 结果超时！' + caller_id)
            return false
        }
        if(g_caller_execute_success === false) {
            console.log('委托撤单 失败！' + caller_id)
            return false
        }
        else {
            console.log('委托撤单 成功。' + caller_id)
            return true
        }
    }
    return true
}

async function QueryCapital(module, query, callback) {
    g_caller_id += 1
    g_caller_execute_success = false
    g_event_call_finish = new AbortController()
    g_caller_dict.set(g_caller_id, new caller_item(g_caller_id))
    let result = JSON.parse(module.DirectCall(g_caller_id, func_future_q_user_capital, 0, query.ToJson(), callback)) // 异步
    console.log(result['return_code'], result['return_info'], result['caller_id'])
    if(result['return_code'] !== 0) {
        return false
    }
    else {
        let caller_id = result['caller_id']
        const ret_wait = await promises.setTimeout(g_caller_wait_time * 1000, '', { signal:g_event_call_finish.signal }).then(() => false, err => true) // 等待调用结果
        if(ret_wait != true) {
            console.log('等待 资金查询 结果超时！' + caller_id)
            return false
        }
        if(g_caller_execute_success === false) {
            console.log('资金查询 失败！' + caller_id)
            return false
        }
        else {
            console.log('资金查询 成功。' + caller_id)
            return true
        }
    }
    return true
}

async function QueryPosition(module, query, callback) {
    g_caller_id += 1
    g_caller_execute_success = false
    g_event_call_finish = new AbortController()
    g_caller_dict.set(g_caller_id, new caller_item(g_caller_id))
    let result = JSON.parse(module.DirectCall(g_caller_id, func_future_q_user_position, 0, query.ToJson(), callback)) // 异步
    console.log(result['return_code'], result['return_info'], result['caller_id'])
    if(result['return_code'] !== 0) {
        return false
    }
    else {
        let caller_id = result['caller_id']
        const ret_wait = await promises.setTimeout(g_caller_wait_time * 1000, '', { signal:g_event_call_finish.signal }).then(() => false, err => true) // 等待调用结果
        if(ret_wait != true) {
            console.log('等待 持仓查询 结果超时！' + caller_id)
            return false
        }
        if(g_caller_execute_success === false) {
            console.log('持仓查询 失败！' + caller_id)
            return false
        }
        else {
            console.log('持仓查询 成功。' + caller_id)
            return true
        }
    }
    return true
}

async function Test_TradeX_Trader_Future_CTP() {
    let kernel = new cyberx.Kernel(new syscfg.SysCfg()) // 全局唯一
    let module = new cyberx.Create('tradex_trader_future_ctp') // 全局唯一
    //let module_01 = new cyberx.Create('tradex_trader_future_ctp') // 重复创建会报异常
    //let module_01 = new cyberx.GetCreate('tradex_trader_future_ctp') // 可以获取已创建的实例
    
    let subscribe_id_01 = module.SubscribeInfo(OnReturnInfo_01) // 订阅信息
    //let subscribe_id_02 = module.SubscribeInfo(OnReturnInfo_02) // 订阅信息
    
    let deploy = new deploy_future_ctp()
    
    let config = new config_trade(deploy)
    
    let result = null
    
    result = await TraderStart(module, config, OnTraderStart)
    console.log(result)
    
    if(true) {
        g_order = new order_item(g_strategy, g_session, 'IF2512', 'CFFE', def_trade_entr_type_l, def_trade_exch_side_b, def_trade_offset_o, def_trade_hedge_s, 4620.0, 1)
        result = await PlaceOrder(module, g_order, OnPlaceOrder)
        console.log(result)
        
        await promises.setTimeout(g_caller_wait_time * 1000, '', {})
        
        console.log('g_order.task_id: ' + g_order.task_id)
        
        result = await CancelOrder(module, g_order, OnCancelOrder)
        console.log(result)
        
        await promises.setTimeout(g_caller_wait_time * 1000, '', {})
    }
    
    if(false) {
        g_query = new query_item(g_strategy, g_session, '', '', '', def_trade_task_query_type_easy)
        result = await QueryCapital(module, g_query, OnQueryCapital)
        console.log(result)
        
        await promises.setTimeout(g_caller_wait_time * 1000, '', {})
    }
    
    if(false) {
        g_query = new query_item(g_strategy, g_session, '', '', '', def_trade_task_query_type_easy)
        result = await QueryPosition(module, g_query, OnQueryPosition)
        console.log(result)
        
        await promises.setTimeout(g_caller_wait_time * 1000, '', {})
    }
    
    config.trade_session = g_session //
    result = await TraderStop(module, config, OnTraderStop)
    console.log(result)
    
    await promises.setTimeout(g_caller_wait_time * 1000, '', {})
    
    module.UnsubscribeInfo(subscribe_id_01) // 退订信息
    //module.UnsubscribeInfo(subscribe_id_02) // 退订信息
}

Test_TradeX_Trader_Future_CTP()
