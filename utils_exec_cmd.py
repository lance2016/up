# import traceback
import base64
import datetime
import json
import subprocess

from dp2_backend_engine.config import config
from dp2_backend_engine.utils.utils_file import save_script


def exec_oscmd(param_config):
    task_param = param_config["task_param"]
    res = ""
    is_success = 0
    cmd = ("ssh {shell_config} {cmd}; echo $? 2>&1").format(
        shell_config=config.get_settings().shell_config,
        cmd=str(task_param["script"]))
    print("CMD : ", cmd)
    res = subprocess.check_output(cmd, stderr=subprocess.STDOUT, shell=True)
    res = res.decode(encoding="utf-8")
    try:
        code = res.strip().split("\n")[-1]
        is_success = 0 if int(code) == 0 else -1
    except Exception as e:
        # err = traceback.format_exc()
        is_success = -1
        res = "命令执行超时 --> " + str(e)
    finally:
        now_time = datetime.datetime.now().strftime("%Y%m%d%H%M%S%f")
        log_dir = str(param_config["data_ver"]) + "_" + str(param_config["task_id"]) + "_" + now_time
        log_path = save_script(log_dir, res)

    # 考虑到数据库容量，最多返回1000个字符
    return_info = res[-1000:]
    return {
        "task_id": param_config["task_id"],
        "data_ver": param_config["data_ver"],
        "runtime_id": param_config["runtime_id"],
        "execute_result": str(
            base64.b64encode(
                bytes(
                    json.dumps(
                        {"return_flag": is_success, "return_info": return_info, "log_path": log_path}
                    ),
                    encoding="utf-8",
                )
            )
        )[1:].replace("'", "")
    }

