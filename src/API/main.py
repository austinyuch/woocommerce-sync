#run in cloud vm:  uvicorn --host 0.0.0.0 --port 7000 main:app
# run in local: uvicorn main:app --reload
# swagger/docs: http://127.0.0.1:7000/docs

import os, sys
import traceback
# import threading
import uvicorn
from pathlib import Path

# PATH_APISERVER = Path(os.getcwd()).joinpath('apiServer')
# str_path_apiserver = str(PATH_APISERVER)
# sys.path.append(str_path_apiserver)
import api
import confs as cfg

if __name__ == "__main__":
    # cloud hosting 使用下面方式
    # uvicorn.run(app="main:app", host="0.0.0.0", reload=True, debug=True,log_level="info")
    # uvicorn.run(app='main:app', host="127.0.0.1", port=8000, log_level="info")
    try:
        uvicorn.run(
            app="api:app",
            host=cfg.dic_api["host"],
            port=cfg.dic_api["port"],
            workers=cfg.dic_api["workers"],
            log_level=cfg.dic_api["log_level"],
            reload=cfg.dic_api["reload"],
            debug=cfg.dic_api["debug"]
        )
    except KeyboardInterrupt:
        print(f'\nExiting\n')
    except Exception as e:
        print(f'Failed to Start API')

        traceback.print_exc(file=sys.stdout)

        print('Exiting\n')
