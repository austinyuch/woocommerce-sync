import asyncio
import os
import json
import copy
import gc
import re
import secrets
import threading
import time
import hashlib
# import numpy as np
# import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path
from urllib.parse import quote

# fastapi相關模組
from pydantic import BaseModel, ValidationError
from pydantic.types import StrBytes
from typing import List
from typing import Optional
from fastapi import FastAPI, Header, Request, Form, Cookie
from fastapi import Depends, HTTPException, status, BackgroundTasks
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware

#安全性
from fastapi.security import HTTPBasic, HTTPBasicCredentials 
from fastapi.openapi.docs import get_swagger_ui_html
from fastapi.openapi.utils import get_openapi


from jose import JWTError, jwt
from passlib.context import CryptContext

#靜態網頁與模板
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
# redirect to certain page as fallback
from fastapi.responses import RedirectResponse

#設定檔
import cred_conf
import confs as cfg

# pop up messging with session
from starlette.middleware import Middleware
from starlette.middleware.sessions import SessionMiddleware # for popup message
# from starlette.templating import Jinja2Templates
middleware = [
    Middleware(SessionMiddleware, secret_key=cred_conf.ACCOUNT_AUTH_SECRET["SECRET_KEY"]),
]
# app = FastAPI(middleware=middleware)
#pop-up messging https://medium.com/@arunksoman5678/fastapi-flash-message-like-flask-f0970605031a

#自製模組   
import html_util

from backup_files import backup_files
from site_wide_cache_helper import update_site_wide_cache, generate_sitemap_xml, \
    cron_update_cache,wrap_cron_update_cache

from api_util import get_economic_indicator,etl_lst_dic_tableau_figure, get_homepage_section,\
    get_blog_page, get_common_dic_page,plural_formatting,etl_i18n,DIC_LANGUAGES,\
    update_html_content_file,get_homepage_dic_result,get_html_page_dic_result,\
    get_html_page_dic_result, get_homepage_dic_result,get_lst_page_template_category

import util.loggingsys


log = loggingsys.generate_general_my_log(log_name=__name__,
                                        log_level=cfg.GLOBAL_LOG_LEVEL,
                                        interval="d")
log_html = loggingsys.generate_general_my_log(log_name=f"html_{__name__}",
                                        log_level=cfg.GLOBAL_LOG_LEVEL,
                                        interval="d")
log_backup = loggingsys.generate_general_my_log(log_name=f"backup_{__name__}",
                                        log_level=cfg.GLOBAL_LOG_LEVEL,
                                        interval="d")
log_page = loggingsys.generate_general_my_log(log_name=f"{__name__}_page",
                                            log_level=cfg.GLOBAL_LOG_LEVEL,
                                            interval="d")
log_cache = loggingsys.generate_general_my_log(log_name=f"{__name__}_cache",
                                            log_level=cfg.GLOBAL_LOG_LEVEL,
                                            interval="d")
log_cache_api = loggingsys.generate_general_my_log(log_name=f"{__name__}_cache_api",
                                            log_level=cfg.GLOBAL_LOG_LEVEL,
                                            interval="d")                                            

# 登入驗證用的加解密key
# SECRET_KEY = cfg.ACCOUNT_AUTH_SECRET["SECRET_KEY"]
# ALGORITHM = cfg.ACCOUNT_AUTH_SECRET["ALGORITHM"]
# ACCESS_TOKEN_EXPIRE_MINUTES = cfg.ACCOUNT_AUTH_SECRET["ACCESS_TOKEN_EXPIRE_MINUTES"] #240分鐘; 預設為30
# ACTIVATION_TOKEN_EXPIRE_MINUTES = cfg.ACCOUNT_AUTH_SECRET["ACTIVATION_TOKEN_EXPIRE_MINUTES"] #1440分鐘; 預設為30

########################################################
#全域變數設定
# pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
# oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")

isMemCache = cfg.dic_api['isMemCache']
isDryRun = cfg.dic_api['isDryRun']
# dic_cache = cfg.DIC_API_CACHE

PATH_DIST =  Path(__file__).parents[1]

origins = cfg.lst_origins

import typing
PathLike = typing.Union[str, "os.PathLike[str]"]

# app = FastAPI()

# static放在root, 並跟api並存, ref https://github.com/tiangolo/fastapi/issues/1516
#https://fastapi.tiangolo.com/advanced/sub-applications/#technical-details-root_path

class HTMLFile(BaseModel):
    filename:str
    strHTML:str
    lang:str=cfg.DEFAULT_LOCALE

########################################################

security=HTTPBasic()

# 保護api docs: 採用基本帳密認證

def verify_user_core(credentials):
    dic_auth = cred_conf.DIC_AUTH
    is_username_correct = False
    is_password_correct = False

    is_username_correct = credentials.username in cred_conf.DIC_AUTH
    if is_username_correct:
        base_password = cred_conf.DIC_AUTH[credentials.username]
    else:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate":"Basic"},
        )

    is_password_correct = secrets.compare_digest(credentials.password,base_password)
    
    if not (is_username_correct and is_password_correct):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate":"Basic"},
        )
    return credentials.username

def get_api_doc_username(credentials: HTTPBasicCredentials=Depends(security)):
    return verify_user_core(credentials)
    

async def verify_username(request: Request) -> HTTPBasicCredentials:

    credentials = await security(request)
    
    return verify_user_core(credentials)

# 網頁全站登入時，需要登入帳密
class AuthStaticFiles(StaticFiles):
    def __init__(self, *args, **kwargs) -> None:

        super().__init__(*args, **kwargs)

    async def __call__(self, scope, receive, send) -> None:

        assert scope["type"] == "http"
        # print(f'{scope=}')
        # print(f'{receive=}')
        request = Request(scope, receive)
        # print(f'{request=}')
        await verify_username(request)
        # await get_api_doc_username(request)
        await super().__call__(scope, receive, send)

app = FastAPI(docs_url=None,
            redoc_url=None,
            openapi_url=None,
            middleware=middleware)

app.mount(f"/{cfg.STR_STATIC_CATEGORY_PREFIX}",         
        StaticFiles(directory=cfg.PATH_STATIC_FOLDER, html = True),
        name=cfg.STR_STATIC_CATEGORY_PREFIX
        )

app.add_middleware(GZipMiddleware, minimum_size=1000)
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

async def secure_headers(request:Request, call_next):
    response = await call_next(request)
    response.headers["Cache-Control"] = "no-cache, no-store"
    response.headers["X-Content-Type-Options"]="nosniff"
    response.headers["X-XSS-Protection"]="1; mode=block"
    response.headers['Access-Control-Allow-Methods']= 'GET,POST,DELETE'
    # response.headers["X-Frame-Options"]="DENY"
    return response

@app.middleware("http")
async def add_my_headers(request: Request, call_next):
    response = await secure_headers(request, call_next)
    
    # response.headers["Access-Control-Allow-Origin"]=
    return response


# 因為前面指定了根目錄host html, 所以這裡靜態路徑都無效

STR_PATH_TEMPLATE_FOLDER = str(cfg.PATH_TEMPLATE_FOLDER.resolve())
templates = Jinja2Templates(directory=STR_PATH_TEMPLATE_FOLDER)
# templates = Jinja2Templates(directory='templates')
templates.env.lstrip_blocks = True
templates.env.trim_blocks = True


# i18n, with Babel and jinjia2 template
# import babel
# https://github.com/PhraseApp-Blog/fastapi-i18n/blob/master/myapp_rental.py
# https://phrase.com/blog/posts/fastapi-i18n/

# assign filter to Jinja2
templates.env.filters['plural_formatting'] = plural_formatting
 
"""
pop-up message通知使用者相關訊息
"""

def flash(request: Request, message: typing.Any, category: str = "") -> None:
    if "_messages" not in request.session:
        request.session["_messages"] = []
    request.session["_messages"].append({"message": message, "category": category})


def get_flashed_messages(request: Request):
    # print(request.session)
    return request.session.pop("_messages") if "_messages" in request.session else []

templates.env.globals['get_flashed_messages'] = get_flashed_messages


"""
OpenAPI Documentation protection
"""
@app.get("/docs", include_in_schema=False)
async def get_documentation(username:str=Depends(get_api_doc_username)):
    return get_swagger_ui_html(openapi_url="/openapi.json",title="docs")

@app.get("/openapi.json", include_in_schema=False)
async def openapi(username:str=Depends(get_api_doc_username)):
    str_ver = cfg.dic_api["last_update"]
    return get_openapi(title="ITRI Tableau FastAPI",version=str_ver,routes=app.routes)

@app.get("/",response_class=HTMLResponse)
async def get_homepage(
                    request:Request,
                    # lang:str="zh",
                    # username:str=Depends(verify_username)
                    lang: Optional[str] = Cookie(default=cfg.DEFAULT_LOCALE),
                    username:str=Depends(get_api_doc_username)
                )->HTMLResponse:
    """
    :param lang:str="en", 回傳指定語系的資料 \n
    :return: html
    """
    try:
        lang_etl = etl_i18n(lang)
        str_this_url = f"{cfg.dic_api['site_url']}{request.url.path}"      
        try:
            dic_result = await get_homepage_dic_result(str_this_url,
                                                    lang_etl=lang_etl,
                                                    use_cache=cfg.dic_api["use_cache"],
                                                    force_update_cache=cfg.dic_api["force_update_cache"],
                                                    lazy_update_cache=cfg.dic_api["lazy_update_cache"]
                                                    )

        except Exception as e:
                log_page.error(f"get_homepage get_homepage_dic_result error {e}")
                dic_result = {}
                
        if "request" not in dic_result:
            dic_result["request"]  = request
        
        # this_tempalte = cfg.DIC_CATEGORY_TEMPLATE[category]
        try:
            ret_page = templates.TemplateResponse(
                "index.html", 
                dic_result
            )
            return ret_page

        except Exception as e:
            log_page.error(f"get_homepage render template error {e}")
            raise HTTPException(status_code=400, detail="Get data failed.")

    except Exception as e:
        str_msg = f"get_homepage error {e}"
        log_page.error(str_msg)
        # raise HTTPException(status_code=401, 
                            # detail="get get_html_page error")
                            # headers={"WWW-Authenticate": "Bearer"})
        return templates.TemplateResponse('missing.html', {'request': request})

@app.get("/api/")
def read_api_root():    
    return cfg.dic_api