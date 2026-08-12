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
// 1、演示 tradex_trader_stock_ufs 交易接口插件的使用；
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

let func_stock_o_user_login    = 1101 // 期货用户登录
let func_stock_o_user_logout   = 1102 // 期货用户登出
let func_stock_t_single_order  = 1201 // 单个证券委托下单
let func_stock_t_single_cancel = 1202 // 单个证券委托撤单
let func_stock_q_user_capital  = 1302 // 查询客户资金
let func_stock_q_user_position = 1304 // 查询客户持仓
let func_stock_r_cancel        = 1900 // 撤单回报
let func_stock_r_order         = 1901 // 报单回报
let func_stock_r_trans         = 1902 // 成交回报

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
let g_strategy = 'stock_ufs'
let g_caller_dict = new Map()
let g_caller_execute_success = false

class deploy_stock_ufs {
    constructor() {
        this.flag = 'stock_ufs'
        this.addr = '10.0.7.200'
        this.port = 8011
        this.trader = 'tradex_trade_stock_ufs'
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
    constructor(strategy, session, symbol, exchange, entr_type, exch_side, price, amount, order_flag = 0) {
        this.order_id = 0 // 委托编号
        this.batch_id = 0 // 批量委托编号
        this.symbol = symbol // 证券代码
        this.exchange = exchange // 交易所，SH:上交所，SZ:深交所
        this.entr_type = entr_type // 委托方式，A股，1:限价，2:市价，H股，1:竞价限价盘，2:增强限价盘，3:零股买卖
        this.exch_side = exch_side // 交易类型，A股，1:买入，2:卖出，29:申购，30:赎回，37:质押入库，38:质押出库，H股，1:买入，2:卖出
        this.price = price // 委托价格
        this.amount = amount // 委托数量 // 恒生UFS柜台申赎数量单位为篮子数即多少个最小申赎单位
        this.fill_qty = 0 // 成交数量
        this.cxl_qty = 0 // 撤单数量
        this.finish_qty = 0 // 完成数量 // 仅供测试交易
        // 0：未申报，1：正在申报，2：已申报未成交，3：非法委托，4：撤单失败/申请资金授权中，5：部分成交，6：全部成交，7：部成部撤，8：全部撤单，9：撤单未成，10：等待撤单
        this.status = 0 // 申报结果
        this.status_msg = '' // 申报说明 // 中文
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
    constructor(strategy, session, order_id, symbol, exchange, query_type) {
        this.order_id = order_id // 委托编号
        this.symbol = symbol // 证券代码
        this.exchange = exchange // 交易所，SH:上交所，SZ:深交所
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
            if(func === func_stock_r_order) {
                console.log('报单回报:', data['order_id'], data['symbol'], data['exchange'], data['exch_side'], 
                                        data['fill_qty'], data['status'], data['status_msg'], data['session'], data['strategy'])
            }
            else if(func === func_stock_r_cancel) {
                console.log('撤单回报:', data['order_id'], data['symbol'], data['exchange'], data['exch_side'], 
                                        data['fill_qty'], data['status'], data['status_msg'], data['session'], data['strategy'])
            }
            else if(func === func_stock_r_trans) {
                console.log('成交回报:', data['order_id'], data['trans_id'], data['symbol'], data['exchange'], 
                                        data['exch_side'], data['fill_qty'], data['fill_price'], data['fill_time'], data['session'], data['strategy'])
            }
            else if(func === func_stock_t_single_order) {
                console.log('报单应答:', data['status'], data['finish'], data['message'], data['session'], data['strategy'])
            }
            else if(func === func_stock_t_single_cancel) {
                console.log('撤单应答:', data['status'], data['finish'], data['message'], data['session'], data['strategy'])
            }
            else if(func === func_stock_q_user_capital) {
                console.log('资金应答:')
                if(data.length > 0) {
                    data.forEach(function(capital) {
                        console.log('account:', capital['account'], 'currency:', capital['currency'], 'available:', capital['available'], 'balance:', capital['balance'])
                    })
                }
                else {
                    console.log('无资金记录。')
                }
            }
            else if(func === func_stock_q_user_position) {
                console.log('持仓应答:')
                if(data.length > 0) {
                    data.forEach(function(position) {
                        console.log('holder:', position['holder'], 'exchange:', position['exchange'], 'currency:', position['currency'], 'symbol:', position['symbol'], 
                                    'security_qty:', position['security_qty'], 'can_sell:', position['can_sell'], 'can_sub:', position['can_sub'], 'exch_side:', position['exch_side'])
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
    let result = JSON.parse(module.DirectCall(g_caller_id, func_stock_o_user_login, 0, config.ToJson(), callback)) // 异步
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
    let result = JSON.parse(module.DirectCall(g_caller_id, func_stock_o_user_logout, 0, config.ToJson(), callback)) // 异步
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
    let result = JSON.parse(module.DirectCall(g_caller_id, func_stock_t_single_order, 0, order.ToJson(), callback)) // 异步
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
    let result = JSON.parse(module.DirectCall(g_caller_id, func_stock_t_single_cancel, 0, order.ToJson(), callback)) // 异步
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
    let result = JSON.parse(module.DirectCall(g_caller_id, func_stock_q_user_capital, 0, query.ToJson(), callback)) // 异步
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
    let result = JSON.parse(module.DirectCall(g_caller_id, func_stock_q_user_position, 0, query.ToJson(), callback)) // 异步
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

async function Test_TradeX_Trader_Stock_UFS() {
    let kernel = new cyberx.Kernel(new syscfg.SysCfg()) // 全局唯一
    let module = new cyberx.Create('tradex_trader_stock_ufs') // 全局唯一
    //let module_01 = new cyberx.Create('tradex_trader_stock_ufs') // 重复创建会报异常
    //let module_01 = new cyberx.GetCreate('tradex_trader_stock_ufs') // 可以获取已创建的实例
    
    let subscribe_id_01 = module.SubscribeInfo(OnReturnInfo_01) // 订阅信息
    //let subscribe_id_02 = module.SubscribeInfo(OnReturnInfo_02) // 订阅信息
    
    let deploy = new deploy_stock_ufs()
    
    let config = new config_trade(deploy)
    
    let result = null
    
    result = await TraderStart(module, config, OnTraderStart)
    console.log(result)
    
    if(false) {
        g_order = new order_item(g_strategy, g_session, '600000', 'SH', def_trade_entr_type_l, def_trade_exch_side_b, 400.0, 100)
        result = await PlaceOrder(module, g_order, OnPlaceOrder)
        console.log(result)
        
        await promises.setTimeout(g_caller_wait_time * 1000, '', {})
        
        console.log('g_order.task_id: ' + g_order.task_id)
        
        result = await CancelOrder(module, g_order, OnCancelOrder)
        console.log(result)
        
        await promises.setTimeout(g_caller_wait_time * 1000, '', {})
    }
    
    if(false) {
        g_query = new query_item(g_strategy, g_session, 0, '', '', def_trade_task_query_type_easy)
        result = await QueryCapital(module, g_query, OnQueryCapital)
        console.log(result)
        
        await promises.setTimeout(g_caller_wait_time * 1000, '', {})
    }
    
    if(true) {
        g_query = new query_item(g_strategy, g_session, 0, '', '', def_trade_task_query_type_easy)
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

Test_TradeX_Trader_Stock_UFS()
