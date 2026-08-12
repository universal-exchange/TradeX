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
// 1、演示 tradex_center 交易中心父级插件的使用；
// 2、依赖交易接口子级插件：tradex_client_future_uff、tradex_client_stock_ufs；
// 3、演示 DirectCall 的同步和异步调用；
// 4、演示 SubscribeInfo 和 UnsubscribeInfo 回调信息订阅退订；

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

let func_client_start   = 1
let func_client_stop    = 2
let func_get_trade_data = 3

let func_trade_data_test = 0 // 调试

let make_call_id = 0 // 递增
let call_wait_time = 10 // 秒
let success_client_start_stop = false

let event_call_finish = null // AbortController

class deploy_stock_ufs {
    constructor() {
        this.flag = 'stock_ufs'
        this.addr = '10.0.7.200'
        this.port = 8011
        this.client = 'tradex_client_stock_ufs'
    }
}

class deploy_future_uff {
    constructor() {
        this.flag = 'future_uff'
        this.addr = '10.0.7.200'
        this.port = 8021
        this.client = 'tradex_client_future_uff'
    }
}

class config_trade {
    constructor(deploy) {
        this.trade_flag = deploy.flag
        this.trade_addr = deploy.addr
        this.trade_port = deploy.port
        this.trade_client = deploy.client
    }
    
    ToJson() {
        return JSON.stringify(this)
        //return JSON.stringify(this, null, 4)
    }
}

function OnClientStart() {
    try {
        let [result] = Array.from(arguments)
        if(result['return_code'] !== 0) {
            success_client_start_stop = false
            console.log(result['return_code'], result['return_info'])
        }
        else {
            success_client_start_stop = true
            let result_data = JSON.parse(result['result_data'])
            console.log('ClientStart:', result['return_info'], result_data)
        }
    }
    catch(error) {
        console.log('OnClientStart 异常！' + error)
    }
    event_call_finish.abort() //
}

function OnClientStop() {
    try {
        let [result] = Array.from(arguments)
        if(result['return_code'] !== 0) {
            success_client_start_stop = false
            console.log(result['return_code'], result['return_info'])
        }
        else {
            success_client_start_stop = true
            let result_data = JSON.parse(result['result_data'])
            console.log('ClientStop:', result['return_info'], result_data)
        }
    }
    catch(error) {
        console.log('OnClientStop 异常！' + error)
    }
    event_call_finish.abort() //
}

async function ClientStart(module, config, callback) {
    make_call_id += 1
    success_client_start_stop = false
    event_call_finish = new AbortController()
    let result = JSON.parse(module.DirectCall(make_call_id, func_client_start, 0, config.ToJson(), callback)) // 异步
    console.log(result['return_code'], result['return_info'], result['caller_id'])
    if(result['return_code'] !== 0) {
        return false
    }
    else {
        let caller_id = result['caller_id']
        const ret_wait = await promises.setTimeout(call_wait_time * 1000, '', { signal:event_call_finish.signal }).then(() => false, err => true) // 等待调用结果
        if(ret_wait != true) {
            console.log('等待 交易启用 结果超时！' + caller_id)
            return false
        }
        if(success_client_start_stop === false) {
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

async function ClientStop(module, config, callback) {
    make_call_id += 1
    success_client_start_stop = false
    event_call_finish = new AbortController()
    let result = JSON.parse(module.DirectCall(make_call_id, func_client_stop, 0, config.ToJson(), callback)) // 异步
    console.log(result['return_code'], result['return_info'], result['caller_id'])
    if(result['return_code'] !== 0) {
        return false
    }
    else {
        let caller_id = result['caller_id']
        const ret_wait = await promises.setTimeout(call_wait_time * 1000, '', { signal:event_call_finish.signal }).then(() => false, err => true) // 等待调用结果
        if(ret_wait != true) {
            console.log('等待 交易停用 结果超时！' + caller_id)
            return false
        }
        if(success_client_start_stop === false) {
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

function OnReturnInfo_01() {
    try {
        let [result] = Array.from(arguments)
        if(result['type'] === msg_func_return_info_log) {
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
        if(result['type'] === msg_func_return_info_log) {
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

async function GetTradeData_Test(module) {
    for(let i = 0; i < call_wait_time; i++) {
        let config_get_trade_data = {'trade_type':func_trade_data_test, 'trade_exchange':'CFFEX'}
        let result = JSON.parse(module.DirectCall(0, func_get_trade_data, 0, JSON.stringify(config_get_trade_data))) // 同步
        if(result['return_code'] !== 0) {
            console.log(result['return_code'], result['return_info'])
        }
        else {
            let trade_data = result['result_data']
            console.log(trade_data['trade_type'], trade_data['trade_exchange'])
        }
        await promises.setTimeout(1000, '', {})
    }
}

async function Test_TradeX_Center() {
    let kernel = new cyberx.Kernel(new syscfg.SysCfg()) // 全局唯一
    let module = new cyberx.Create('tradex_center') // 全局唯一
    //let module_01 = new cyberx.Create('tradex_center') // 重复创建会报异常
    //let module_01 = new cyberx.GetCreate('tradex_center') // 可以获取已创建的实例
    let subscribe_id_01 = module.SubscribeInfo(OnReturnInfo_01) // 订阅信息
    let subscribe_id_02 = module.SubscribeInfo(OnReturnInfo_02) // 订阅信息
    
    let deploy = new deploy_stock_ufs()
    //let deploy = new deploy_future_uff()
    
    let config = new config_trade(deploy)
    
    let result = null
    
    result = await ClientStart(module, config, OnClientStart)
    console.log(result)
    
    await GetTradeData_Test(module)
    
    //await promises.setTimeout(call_wait_time * 1000, '', {})
    
    result = await ClientStop(module, config, OnClientStop)
    console.log(result)
    
    await promises.setTimeout(call_wait_time * 1000, '', {})
    
    module.UnsubscribeInfo(subscribe_id_01) // 退订信息
    module.UnsubscribeInfo(subscribe_id_02) // 退订信息
}

Test_TradeX_Center()
