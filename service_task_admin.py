# -*- coding: utf-8 -*-
# @Modify Time: 2022-03-28
# @Author      : feilong.chen
# @Modify Time : 2022-04-05
# @Author      : xun.jiang
# @comment     : 重构，SQL部分放到model_task.py
from __future__ import unicode_literals

import datetime
import json

from config import config
from dp2_backend_schedualer.dp_schedule.model import model_task_instance, model_task_runtime_log
from dp2_backend_schedualer.dp_task.model import (model_tag_rel, model_task,
                                                  model_task_dag)
from dp2_backend_utils.utils import utils_db
from utils.utils_file import save_script

UTILS_DB = utils_db.UTILS_DB()


# 创建任务(包含ods/etl/quality)
def insert_task_detail(db_info, task_info: dict) :

    # 整理任务的参数
    get_save_task_parm(db_info, task_info)
    # 创建任务
    insert_rs = model_task.insert_task(db_info, task_info)
    task_id = insert_rs["task_id"]
    task_info["task_id"] = task_id
    # 处理第一次上线是否跑指定重跑时间/自增字段起始值
    if task_info["bussiness_config"].find("inc_start") != -1:
        write_config_last_val(db_info, task_info)
    # 关联其它操作
    rel_rs_bool = task_rel_other(db_info, task_info)
    if rel_rs_bool is False:
        return None
    return insert_rs


def get_save_task_parm(db_info, task_info):
    task_type = task_info["task_type"]
    # task_info["func_name"] = FUNC_DICT.get(task_type, "")
    # task_info["create_user"] = task_info.get("create_user", "admin")

    # if task_type == "QUALITY":
    #     db_tag_id = model_task.get_data_source_tag_by_id(
    #         db_info, task_info["instance_id"]
    #     )
    #     task_info["db_tag"] = db_tag_id
    #     task_info["param_config"] = get_quality_param_config(task_info)
    #     task_info["param_config"]["is_table_config"] = True
    param_config = task_info["param_config"]

    if task_type == "OSCMD":
        script = param_config["script"]
        file_name = save_script(task_info["task_name"], script)
        # task_info["func_name"] = "exec_oscmd"
        param_config["file_name"] = file_name
        param_config["url"] = config.get_settings().server_scheduler + "/static/" + file_name

    # 设置 db_config
    setup_db_config(db_info, param_config, task_info)

    # 设置 is_dblog
    setup_is_dblog(db_info, param_config, task_info)

    # ods根据bussiness_config参数追加param_config参数
    if task_type == "ODS":
        add_param_config(param_config, task_info["bussiness_config"])
        # 开启数据血缘保存功能
        param_config["is_save_lineage"] = "on"

    # 换行符替换,防止读取换行错误/数据库报错
    dbType = db_info["db_type"]
    param_config_str = json.dumps(task_info["param_config"], ensure_ascii=False)
    task_info["param_config"] = (
        UTILS_DB.escape_function(dbType, param_config_str)
    )
    if task_type == "PROC":
        func_name_str = json.dumps(task_info["func_name"], ensure_ascii=False)
        task_info["func_name"] = (
            UTILS_DB.escape_function(dbType, func_name_str)
        )
    # 换行符替换,防止读取换行错误/数据库报错
    bussiness_config = "null"
    if task_info.get("bussiness_config") is not None:
        bussiness_config = json.dumps(task_info["bussiness_config"])
    task_info["bussiness_config"] = (
        UTILS_DB.escape_function(dbType, bussiness_config)
    )

    # 单次执行时，需要将最早开始时间的’年月日‘和‘时分秒’拆开。start_time设置为‘年月日000000’，最早开始时间只保留‘时分秒’
    if "cycle_type" in task_info and task_info["cycle_type"] == 'SINGLE':
        earl_start_time = task_info["earliest_start_time"]
        start_time = "{}000000".format(earl_start_time[0:10].replace("-", ""))
        task_info["day_start_time"] = start_time
        task_info["day_end_time"] = start_time
        task_info["earliest_start_time"] = earl_start_time[11:]

    # 如果以下字段为空，需添加默认时间
    task_info["day_start_time"] = task_info.get(
        "day_start_time", datetime.datetime.now().strftime("%Y%m%d") + "000000"
    )
    task_info["day_end_time"] = task_info.get(
        "day_end_time", datetime.datetime.now().strftime("%Y%m%d") + "000000"
    )


def add_param_config(param_config, bussiness_config):
    # 处理用户字段选择, 自定义目标字段名称、自定义目标字段中文名注释
    if "col_choose" in bussiness_config and len(bussiness_config["col_choose"]) > 0:
        cols = bussiness_config["col_choose"]
        black_column_list = []  # 黑名单字段(非同步)
        col_map_list = [] # 源表字段-目标字段
        # col_cn_map_list = [] # 目标字段-目标字段中文注释
        for col in cols:
            sr_col = col["column_name_en"]
            if col["is_syn"] == "on":
                targt_col = col["target_column_name_en"]
                col_map_list.append(f"{sr_col}:{targt_col}")
                # targt_col_cn = col["target_column_name_cn"]
                # col_cn_map_list.append(f"{targt_col}:{targt_col_cn}")
            else:
                black_column_list.append(sr_col)
        param_config["black_columns"] = ",".join(black_column_list)
        param_config["to_col_mapping"] = ",".join(col_map_list)
        # param_config["to_col_cn_mapping"] = ",".join(col_cn_map_list)
    # 当不存在主键时，需改变数据加载方式
    if "primary_key" not in param_config and param_config["lake_load"] == "pk_inc":
        param_config["lake_load"] = "pk_inc_fast"

    # 默认不自动启动gpfdist
    param_config["check_gpfdist"] = "False"


def setup_db_config(db_info, param_config, task_info):
    task_type = task_info["task_type"]
    bussiness_config = task_info.get("bussiness_config", dict())

    if task_type == "ODS":
        # 数据源文件改为传名字，便于数据血缘展示
        if bussiness_config["source_type"] == "FILE":
            param_config["db_config"] = model_task.get_excel_config(param_config)
        else:
            param_config["db_config"] = str(task_info["instance_name"]) + ".json"
        # 设置数据源类型
        if "instance_id" in task_info:
            bussiness_config["db_type"] = model_task.get_db_config_type(db_info, task_info["instance_id"])
        # 设置目标数据源
        param_config["to_db_config"] = model_task.get_data_warehouse_config(db_info)
    elif task_type == "QUALITY" and "instance_id" in task_info:
        param_config["db_config"] = "instance_" + str(task_info["instance_id"]) + ".json"
        if "instance_name" in task_info:
            param_config["db_config"] = str(task_info["instance_name"]) + ".json"
    elif task_type in ["PROC", "SQL"]:
        if bussiness_config is not None and "schema" in bussiness_config:
            # 根据schema拿到instansId
            schema_id = bussiness_config["schema"]
            param_config["db_config"] = model_task.get_db_config_by_schema(db_info, schema_id)
        else:
            param_config["db_config"] = model_task.get_data_warehouse_config(db_info)
    elif task_type == "STREAM_CALC":
        set_stream_param_config(param_config, bussiness_config, db_info)


def write_config_last_val(db_info, task_info):
    source_info = model_task.get_sr_config_by_instance(db_info, task_info["instance_id"])
    param_config = json.loads(task_info["param_config"])
    bussiness_config = json.loads(task_info["bussiness_config"])
    if bussiness_config["inc_start"] == "":
        return None
    ods_config_info = {}
    ods_config_info["db_host_source"] = source_info["db_host"]
    ods_config_info["db_name_source"] = source_info["db_name"]
    if "table_name" in param_config:
        ods_config_info["table_name_source"] = param_config["table_name"]
    else:
        # query模式不存在表名，需从query表达式中解析获取
        query = param_config.get("query", None)
        if query is not None:
            import re
            schema_table_str = re.compile("from\s+([\w+|.+|_+]+)\s?", re.IGNORECASE).findall(query)[0]
            ods_config_info["table_name_source"] = schema_table_str.strip()
    ods_config_info["table_name_target"] = param_config["to_table_name"]
    ods_config_info["query_source"] = param_config.get("query", None)
    ods_config_info["inc_column_source"] = param_config.get("inc_key", None)
    inc_start = bussiness_config["inc_start"]
    if len(inc_start) == 14:
        # 长度14为时间类型的字符，如20220707202113
        ods_config_info["last_time_target"] = inc_start
        from dhw3.dp_dump.utils import utils_inc
        utils_inc.reset_last_time_by_task(task_id=task_info["task_id"], ods_config_info=ods_config_info)
    else:
        ods_config_info["last_num_target"] = inc_start
        from dhw3.dp_dump.utils import utils_inc_num
        utils_inc_num.reset_last_num_by_task(task_id=task_info["task_id"], ods_config_info=ods_config_info)


def task_rel_other(db_info, task_info, ) -> bool:
    task_id = task_info["task_id"]
    # 依赖任务标签
    datasource_tag_id_list = []
    if "p_task_list" in task_info and task_info["p_task_list"] is not None:
        p_task_id_list = [p_task['p_task_id'] for p_task in task_info["p_task_list"]]
        datasource_tag_id_list = model_tag_rel.get_tag_id_list(
            db_info, task_id_list=p_task_id_list, tag_type="data_source_tag"
        )
    try:
        # 根据p_task_id创建dag
        insert_task_rel(db_info, task_info)
        if "db_tag" in task_info and task_info["db_tag"] is not None and task_info["db_tag"] != "":
            datasource_tag_id_list.append(task_info["db_tag"])
        unique_datasource_tag_list = list(set(datasource_tag_id_list))
        if len(unique_datasource_tag_list) > 0:
            datasource_tag_ids = ",".join(
                "%s" % id for id in unique_datasource_tag_list
            )
            insert_many_tag_rel(db_info, datasource_tag_ids, task_id, "data_source_tag")
        if (
                "tags" in task_info
                and task_info["tags"] is not None
                and len(task_info["tags"]) > 0
        ):
            # 插入tag关联表
            insert_many_tag_rel(db_info, task_info["tags"], task_id, "task_tag")
    except Exception as e:
        # logger.error(e)
        # 出现异常，删除已创建的任务和dag
        model_task.del_task(db_info, task_id)
        model_task_dag.delete_task_dag_edge(db_info, task_info)
        return False
    return True

def setup_is_dblog(db_info, param_config, task_info):
    task_type = task_info["task_type"]

    # 只有探查任务时，才设置默认的 to_db_config
    if task_type in ("QUALITY"):
        param_config["is_dblog"] = True


# 插入tag_rel
def insert_many_tag_rel(db_info, tag_ids, task_id, tag_type):
    # 先删除再插入
    delete_sql_script = (
        "delete from {db_schema}.cw_scheduler_tag_rel "
        " where task_id = {task_id} and tag_type='{tag_type}'"
        " ".format(db_schema=db_info["db_default_schema"], task_id=task_id, tag_type=tag_type)
    )
    UTILS_DB.exec_db(db_info, delete_sql_script, sqlType="dml")
    if tag_ids is not None and len(tag_ids.strip()) > 0:
        tag_rel_dict = {
            "task_id": task_id,
            "tag_id_list": tag_ids,
            "tag_type": tag_type,
        }
        model_tag_rel.insert_tag_rel(db_info, tag_rel_dict)


# 根据p_task_id创建dag
def insert_task_rel(db_info, task_info):
    task_id = task_info["task_id"]
    p_task_list = task_info.get("p_task_list", None)
    # 因前端会携带[None]值,先清除掉
    if p_task_list != None and len(p_task_list) >= 0:
        list_task_pending = []
        for p_task in p_task_list:
            if p_task["need_params"] == 1:
                list_task_pending.append(
                    (task_id, p_task["p_task_id"], p_task["need_params"], p_task["params"]))
            else:
                list_task_pending.append(
                    (task_id, p_task["p_task_id"], p_task["need_params"], None))
        model_task_dag.add_multi_dag_edge(db_info, task_id, list_task_pending)


# 更新/编辑任务
def update_task(db_info, task_info):
    # 整理任务的参数
    get_save_task_parm(db_info, task_info)
    # 接受入参里这些类型的字段，更新至task表，
    # 与task_info传入的参数取并集，只有两边都存在才更新
    set_type = {
        "task_name": "str",
        "priority": "number",
        "day_start_time": "str",
        "day_end_time": "str",
        "cycle": "str",
        "tag": "str",
        "status": "str",
        "earliest_start_time": "str",
        "func_name": "str",
        "param_config": "str",
        "bussiness_config": "str",
        "task_type": "str",
        "task_info": "str",
        "resource_group": "number",
        "merge_task": "str",
        "instance_type": "str",
        "timeout": "number",
        "fail_over": "str"
    }
    model_task.update_task_db(db_info, task_info, set_type)
    # 处理第一次上线是否跑指定重跑时间/自增字段起始值
    if task_info["bussiness_config"].find("inc_start") != -1:
        write_config_last_val(db_info, task_info)
    task_rel_other(db_info, task_info)


# 下线任务 (默认为强制下线, 有正在运行的实例进程需杀掉)
def offline_task(db_info, task_info):
    from dp2_backend_schedualer.dp_task.service import service_task_run

    task_id = task_info["task_id"]
    task_type = task_info["task_type"]
    # 取消flink任务
    if task_type == "STREAM_CALC":
        log_info = model_task_runtime_log.get_log_by_taskId(db_info, task_id)
        try:
            return_info = json.loads(log_info["return_info"])
            if "job_id" in return_info and return_info["job_id"] not in ("", None, "None", "null"):
                service_task_run.call_dhw3_cancel_interface(return_info["job_id"])
        except BaseException as e:
            pass
    # 如果存在正在运行pid，需强制杀死
    else:
        inst_info = model_task_instance.get_instance_by_id(db_info, task_id)
        if (
            inst_info is not None
            and "pid" in inst_info
            and inst_info["pid"][0] not in (None,"")
        ):
            pid = inst_info["pid"][0]
            is_killed = False; kill_cnt = 1
            # 最多kill三次
            while not is_killed and kill_cnt <=3:
                result = service_task_run.call_dhw3_kill_interface(pid)
                kill_cnt += 1
                if result["code"] == 200 and result["success"] is True:
                    is_killed = True
    # 更新任务日志状态
    model_task_runtime_log.update_running_log_db(
        db_info=db_info,
        task_id=task_id,
        result_dict={
            "task_id": task_id,
            "execute_result": {
                "log_path": "",
                "return_info": "已手动停止运行！",
                "end_time": "now()"
            },
        },
        task_status="fail",
    )
    set_type = {"status": "str"}
    task_info["status"] = "offline"
    # 删除任务实例
    model_task_instance.del_task_instance_init(db_info, task_id)
    # 更新任务状态
    model_task.update_task_db(db_info, task_info, set_type)


# 上线任务
def online_task(db_info, task_info):
    task_info["status"] = "online"
    task_info["day_start_time"] = task_info.get(
        "day_start_time", datetime.datetime.now().strftime("%Y%m%d") + "000000"
    )
    # 更新任务状态
    set_type = {"status": "str", "day_start_time": "str"}
    model_task.update_task_db(db_info, task_info, set_type)
    # 重新上线后(重跑)第一次是否跑全量
    if "is_full_his" in task_info and task_info["is_full_his"] == 'on':
        from dhw3.dp_dump.utils import utils_inc_num

        # TODO 需要考虑数值型重跑
        utils_inc_num.reset_full_his_by_task(task_info["task_id"])
    # 创建任务instance
    model_task_instance.insert_task_instance_init(db_info, task_info)


# 拼接探查任务需要的param_config
def get_quality_param_config(task_info: dict):
    param_config = {}
    param_config["db_config"] = task_info.get("db_config", "")
    param_config["table_name"] = task_info.get("table_name", "")
    param_config["bucket_size"] = task_info.get("bucket_size", "")
    if "white_column_list" in task_info and task_info["white_column_list"] is not None:
        param_config["white_column_list"] = task_info.get("white_column_list", "")
    if "black_column_list" in task_info and task_info["black_column_list"] is not None:
        param_config["black_column_list"] = task_info.get("black_column_list", "")
    return param_config


# 拼接流任务需要的param_config
def set_stream_param_config(param_config, bussiness_config, db_info):
    if "source_connect_id" in bussiness_config:
        source_connect_id = bussiness_config["source_connect_id"]
        param_config["source_connect_config"] = "stream_connector_" + str(source_connect_id) + ".json"
        source_connect_info = model_task.get_stream_connect_info(db_info, source_connect_id)[0]
        param_config["source_connect_sql"] = source_connect_info["flink_create_sql"]
        param_config["source_consume_strategy"] = source_connect_info["consume_strategy"]
        param_config["source_format"] = source_connect_info["out_format"]
    if "sink_connect_id" in bussiness_config:
        sink_connect_id = bussiness_config["sink_connect_id"]
        param_config["sink_connect_config"] = "stream_connector_" + str(sink_connect_id) + ".json"
        sink_connect_info = model_task.get_stream_connect_info(db_info, sink_connect_id)[0]
        param_config["sink_connect_sql"] = sink_connect_info["flink_create_sql"]
        param_config["sink_format"] = sink_connect_info["out_format"]
    if "window_size" in bussiness_config:
        window_size = bussiness_config["window_size"]
        window_unit = bussiness_config.get("window_unit", "s")
        # 时间单位m(分钟级)转换为s(秒级)
        if window_unit == "m":
            window_size *= 60
        param_config["window_size"] = window_size
    if "to_sink" in param_config:
        param_config.pop("to_sink")
