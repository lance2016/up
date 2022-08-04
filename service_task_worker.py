# -*- coding: utf-8 -*-
# @Created Time: 2022-04-04
# @Author      : xun.jiang

import os
from dhw3.dp_worker import worker_exec
import requests
from requests import Response
from dp2_backend_schedualer.config import config

IS_DEBUG = os.getenv("IS_DEBUG", default='false')


# HTTP调用预留接口
def http_call(run_node, run_port, task_type, function_name, param_config) -> Response:
    # 告警、worker指标任务路由到engine节点运行
    settings = config.get_settings()
    if task_type in ["ALARM", "MONITOR", "NOTIFY"]:
        url = f"{settings.server_engine}/worker/run"
    elif task_type == "OSCMD":
        url = f"{settings.server_engine}/worker/exec/cmd"
    else:
        url = "http://{host}:{port}/scheduler/run/task/router_execute_task/".format(
            host=run_node, port=run_port
        )
    params = {
        "run_node": run_node,
        "task_type": task_type,
        "function_name": function_name is not None and function_name or "",
        "param_config": param_config,
        "call_type": "async",
    }
    request_return = requests.request("post", url, json=params, timeout=3600)
    return request_return


# BUG FIX，命令行模式 如果有空格会被割断，需要加 """
def exec_dump_os(run_node, task_type, function_name, param_config):
    cmd = "python /dhw_home/dhw3/dp_dump/dhw_extract.py "
    for key in param_config:
        cmd = cmd + ' --{}="{}"'.format(key, param_config[key])
    print(cmd)
    cmd = "{};echo $?".format(cmd)

    result = os.popen(cmd)
    res = result.read().split("\n")
    # 最后一行是回车符，所以返回-2
    return {"return_flag": res[-2], "return_info": res, "log_path": ""}


def exec_task(run_node, run_port, task_type, function_name, param_config):
    if run_node is not None and IS_DEBUG.lower() == 'false':
        # 调用远程方法
        return http_call(run_node, run_port, task_type, function_name, param_config)
    else:
        print("----start local debug----")
        TASK_RUN_FUN = {
            "ODS": exec_dump_os,
            "QUALITY": worker_exec.exec_task,
            "OSCMD": worker_exec.exec_task,
            "SQL": worker_exec.exec_task,
            "PROC": worker_exec.exec_task,
            "MONITOR": worker_exec.exec_task,
            "ALARM": worker_exec.exec_task,
        }
        # 调用本地方法
        res = TASK_RUN_FUN[task_type](run_node, task_type, function_name, param_config)
        return res
