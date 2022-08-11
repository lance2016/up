import argparse
import datetime
import os
import re

from dhw3.configs.config import get_settings
from dhw3.dp_stream.utils import (utils_remote_manager, utils_sink_ddl,
                                  utils_source_ddl, utils_stream_env)
from dp2_backend_utils.utils import (utils_args, utils_db, utils_error,
                                     utils_log, utils_return, utils_task_state)

UTILS_DB = utils_db.UTILS_DB()
UTILS_STREAM_ENV = utils_stream_env.UTILS_STREAM_ENV()
UTILS_SR_DDL = utils_source_ddl.UTILS_SR_DDL()
UTILS_SINK_DDL = utils_sink_ddl.UTILS_SINK_DDL()

CODE_BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

UTILS_ERROR = utils_error.DHW_ERROR()
UTILS_STATE = utils_task_state.TASK_STATE()

def parseArgs(args):
    parser = argparse.ArgumentParser()
    # Global options
    globalArgs = parser.add_argument_group("Global options")
    # flink transform
    globalArgs.add_argument("--flink_sql", type=str, default=None)
    # flink并发数
    globalArgs.add_argument("--parallel", type=int, default=1)
    # 窗口数据分配规则(默认滚动)
    globalArgs.add_argument("--window_rule", type=str, default="tumble",
                            choices=["tumble", "slide", "session"])
    # 窗口大小(单位默认为秒级)
    globalArgs.add_argument("--window_size", type=int, default=10)

    # 同步模式
    globalArgs.add_argument("--sync_mode", type=str, default="upsert",
                            choices=["append_only", "retract", "upsert"])

    # 数据文件的文件夹
    globalArgs.add_argument("--file_path", type=str, default=None)
    globalArgs.add_argument("--dir_data", type=str, default=None)
    globalArgs.add_argument("--dir_log", type=str, default=None)

    # 源source连接信息
    globalArgs.add_argument("--source_connect_config", type=str, default="stream_connector_kafka.json")
    globalArgs.add_argument("--source_connect_sql", type=str, default=None)
    globalArgs.add_argument("--source_consume_strategy", type=str, default="latest-offset",
                            choices=["group-offsets", "earliest-offset", "latest-offset", "timestamp"])
    globalArgs.add_argument("--source_format", type=str, default="json", choices=["csv", "json"])

    # 以下参数通过流连接器传递
    # # topic名称
    # globalArgs.add_argument("--topic_name", type=str, default=None)
    # # 消费策略

    # # 消费者组
    # globalArgs.add_argument("--consumer_group", type=str, default="cw_consumer_group")
    # # 输出格式
    # globalArgs.add_argument("--out_format", type=str, default="csv", choices=["csv", "json"])

    # # 目标端
    # 目标sink连接信息
    globalArgs.add_argument("--sink_connect_config", type=str, default="py_gp.json")
    globalArgs.add_argument("--sink_connect_sql", type=str, default=None)
    globalArgs.add_argument("--sink_format", type=str, default="json", choices=["csv", "json"])

    # globalArgs.add_argument("--to_sink", type=str, default="csv",
    #                         choices=["greenplum", "csv", "connector"])
    # # 目标schema
    # globalArgs.add_argument("--to_schema", type=str, default="")
    # # 目标表名
    # globalArgs.add_argument("--to_table_name", type=str, default="")
    # # 输出文件路径
    # globalArgs.add_argument("--to_file_path", type=str, default="")
    # # 是否同时创建外部表
    # globalArgs.add_argument("--is_create_ext", type=str, default="off")

    # 数据版本
    globalArgs.add_argument("--data_ver", type=str, default=None)
    # task id
    globalArgs.add_argument("--task_id", type=str, default=None)

    return parser.parse_args(args)


def reset_parameter(args):
    # source配置信息
    args.connectSourceInfo = UTILS_DB.get_db_conncet_info(args.source_connect_config)

    # sink端配置
    args.connectSinkInfo = UTILS_DB.get_db_conncet_info(args.sink_connect_config)
    if args.sink_connect_sql is None:
        source_create_sql = args.source_connect_sql
        source_table_name = get_sr_table_name(source_create_sql)
        args.sink_connect_sql = source_create_sql.replace(source_table_name, args.to_table_name)
    # if "file_name" not in args.connectSinkInfo:
    #     args.connectSinkInfo["file_name"] = "{}_{}.bat".format(args.to_schema, args.to_table_name)
    # if "out_format" not in args.connectSinkInfo:
    #     args.connectSinkInfo["out_format"] = args.connectSinkInfo.get("out_format", "csv")

    # dhw3读取flink数据目录
    # if args.dir_data is None:
    #     args.dir_data = "{}/data_flink/{}/{}".format(os.path.dirname(CODE_BASE), args.job_id,
    #                                                  args.connectSinkInfo["file_name"])
    args.to_table_name = get_sr_table_name(args.sink_connect_sql)

    # sink_file_path (flink服务目录)
    if args.file_path == "":
        flink_sink_file_path = get_settings().flink_sink_file_path
        args.file_path = "{}/{}".format(flink_sink_file_path, args.job_id)

    # dhw3运行flink初始化日志文件夹
    if args.dir_log is None:
        args.dir_log = init_dir_log(args.job_id)
    # 初始化logger
    args.logger = utils_log.get_worker_exec_logger(args.dir_log)

    return args


def stream_task(input_args) -> dict:
    print("stream_task start input_args : ", input_args)
    t_start = datetime.datetime.now()

    dic_return = {}
    try:
        args = utils_args.dic_to_list(input_args)
        args = parseArgs(args)
        args = utils_args.init_job_id(args)
        args = reset_parameter(args)
        dir_log_path = args.dir_log

        # 执行流任务
        stream_result = source_2_sink(args)

        dic_return["return_flag"] = stream_result["return_flag"]
        dic_return["return_info"] = stream_result["return_info"]
        # if args.to_sink == "csv":
        #     dic_return[""]
        args.logger.critical("=" * 30)
        t_end = datetime.datetime.now()
        dur = (t_end - t_start).total_seconds()
        args.logger.critical("Total TimeCost:\t{:.2f} Sec.".format(dur))
        args.logger.critical("=" * 30)
    except BaseException as e:
        import traceback

        err = traceback.format_exc()
        now = datetime.datetime.strftime(datetime.datetime.now(), "%Y-%m-%d %H:%M:%S")
        error_mess = UTILS_ERROR.get_error_message("DHW-10019").format(
            now, input_args.get("task_id"), input_args.get("data_ver"), err
        )

        err_logger, dir_log_path = utils_args.init_logger_by_input_args(input_args, init_dir_log)
        err_logger.critical("+++++++++++++++ extract_task error +++++++++++++++")
        err_logger.critical(error_mess)
        err_logger.critical("=" * 30)

        dic_return["return_flag"] = UTILS_STATE.get_task_state("fail")
        dic_return["return_info"] = error_mess
    finally:
        # 取日志分两种情况：1.任务发送flink成功, 则取flink运行日志; 2.发送flink失败，则取本地日志
        if dic_return["return_flag"] == UTILS_STATE.get_task_state("success"):
            # 为便于前端查看, 流任务的日志状态始终为运行中
            dic_return["return_flag"] = UTILS_STATE.get_task_state("running")
            dic_return["log_path"] = (
                "http://{ip}:{port}/jobs/{job_id}/exceptions"\
                " ".format(
                    ip=get_settings().flink_server_host,
                    port=get_settings().flink_server_port,
                    job_id=stream_result["return_info"]["job_id"]
                )
            )
        else:
            dic_return["log_path"] = "{}".format(dir_log_path)
        # 清理logger
        utils_log.clean_worker_exec_logger(dir_log_path)

    return dic_return


def source_2_sink(args):
    source_ddl = UTILS_SR_DDL.get_source_ddl(args)
    sink_ddl = UTILS_SINK_DDL.get_sink_ddl(args)

    stream_table_env = UTILS_STREAM_ENV.set_stream_table_env(parallel=args.parallel)

    stream_table_env.execute_sql(source_ddl)
    stream_table_env.execute_sql(sink_ddl)

    query_sql = args.flink_sql
    to_table_name = args.to_table_name
    source_table = stream_table_env.sql_query(query_sql)
    source_table.execute_insert(to_table_name)

    job_id = utils_remote_manager.get_job_id(to_table_name)

    exec_rs = {}
    exec_rs["job_id"] = job_id
    return utils_return.get_return_success(exec_rs)


def get_sr_table_name(flink_sql) -> str:
    pattern = re.compile("\s+table\s+", re.I)
    return pattern.split(flink_sql)[1].split("(")[0].strip()


def init_dir_log(job_id):
    dir_log = "{}/log/stream_flink/{}".format(
        os.path.dirname(CODE_BASE), job_id
    )
    return dir_log


if __name__ == "__main__":
    input_args = None
    stream_result = stream_task(input_args)
    print(stream_result)
    exit(0) if stream_result['return_flag'] == 0 else exit(-1)
