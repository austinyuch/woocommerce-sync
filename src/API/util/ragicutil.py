import asyncio
import requests
import pandas as pd
import json
import hashlib
import threading
import datetime
import copy
import time
import numpy as np
# import simplejson

from pathlib import Path
from confs import LST_DROP_RAGIC_COLS,GLOBAL_LOG_LEVEL,RAGIC_TAB_URL ,\
    dic_api
from confs import DIC_RAGIC_TABLE_CONF,PATH_CACHE_FOLDER

from confs import RAGIC_ACCOUNT, RAGIC_PROJECT_NAME

from confs import INPUT_DATE_FMT, OUTPOUT_DATE_FMT

from cred_conf import RagicAPIKey

from etlutil import inv_dict_mapping,dict_col_rename_mapping


import loggingsys
log = loggingsys.generate_general_my_log(log_name=__name__,log_level=GLOBAL_LOG_LEVEL)




async def request_post_n_times(str_api_url, 
                            dic_data, 
                            dic_header,
                            max_trial=3
                        ):
    """
    嘗試n次post 如果回傳為{}就再重複打, 最多嘗試max_trial次
    :param str_api_url
    :param dic_data: 要打上去的資料
    :param dic_header: 如{'Content-type': 'application/json', 'Accept': 'text/plain'}
    :param max_trial: 最大嘗試次數
    :return dict: dict(r.json()["data"])
        {'_ragicId': 18, '_star': False, '_create_date': '2021/08/20 09:40:53',
         '_create_user': 'cameoinfotech.tw@gmail.com', '1000103': '', 
         '_index_title_': '', '1000104': '建立文章t1.html', '1000105': '更新文章t1_u1', 
         '1000106': [], '1000107': '', '1000109': '', '1000110': '貝比斯', '1000111': '',
          '1000112': '2021/08/20', '_index_calDates_': 'd1000112 2021/08/20 17:36:45', 
          '1000136': '', '_index_': ''}
    """
    trial = 0
    # max_trial = 3
    isFail = True
    t_slee = 0.2
    lst_dic_r=[]
    if dic_data == {} or dic_data is None:
        return {}

    while trial < max_trial and isFail == True:
        try:
            r = requests.post(str_api_url, 
                            data=json.dumps(dic_data), 
                            headers=dic_header)
            dic_r = r.json()   
            if dic_r == {} or dic_r['status'] != 'SUCCESS':
                isFail = True     
                time.sleep(t_slee)
            else:
                isFail = False      
                             
        except:
            isFail = True
            time.sleep(t_slee)
        finally:
            trial += 1  
            
    if isFail:
        return {}
        
    else:
        dic_r_json = dict(r.json()["data"])
        return dic_r_json

async def request_delete_n_times(str_api_url, dic_header, max_trial=3):
    """
    嘗試n次post 如果回傳為{}就再重複打, 最多嘗試max_trial次 \n
    :param str_api_url \n
    :param dic_header: 如{'Content-type': 'application/json', 
        'Accept': 'text/plain', 'Authorization':'Basic xxxtoken'}
    :param max_trial: 最大嘗試次數 \n
    :return dict: dict(r.json()["data"])
    """
    trial_count = 0
    # max_trial = 3
    isFail = True
    t_sleep = 0.5
    lst_dic_r=[]
    while trial_count < max_trial and isFail == True:
        try:
            r = requests.delete(str_api_url, headers=dic_header)
            # dic_r = r.json()   
            """
            r.json() = {
            'msg':'The record has been moved to recycle bin.'
            'ragicId':2815}

            """
            # if dic_r == {} or dic_r['status'] != 'SUCCESS':
            if r.status_code == 200:
                isFail = False      
            else:
                isFail = True 
                time.sleep(t_sleep) 


        except:
            isFail = True
            time.sleep(t_sleep) 

        finally:
            trial_count += 1  
            
    return r
   


class RagicTable:
    def __init__(self, 
                host:str,
                account:str,
                project:str, 
                table_id:str,
                str_ragicAPIKey:str=RagicAPIKey):
        table_id=str(table_id)
        # self.RAGIC_TAB_URL = f"{RAGIC_HOST}/{RAGIC_ACCOUNT}/{RAGIC_PROJECT_NAME}"
        self.str_ragicAPIKey=str_ragicAPIKey
        self.ragic_tab_url = f"{host}/{account}/{project}"
        self.table_id = table_id
        self.str_url = f"{self.ragic_tab_url}/{table_id}"
        # self.name = DIC_RAGIC_TABLE_CONF[table_id]["TABLE_NAME"]
        self.dic_tb_col_id = DIC_RAGIC_TABLE_CONF[table_id]["DIC_COL_ID"]
        if "LST_COL_MULTI" in DIC_RAGIC_TABLE_CONF[table_id]:
            self.LST_COL_MULTI = DIC_RAGIC_TABLE_CONF[table_id]["LST_COL_MULTI"]
        else:
            self.LST_COL_MULTI = []
        if "LST_COL_FILE" in DIC_RAGIC_TABLE_CONF[table_id]:
            self.LST_COL_FILE = DIC_RAGIC_TABLE_CONF[table_id]["LST_COL_FILE"]
        else:
            self.LST_COL_FILE = []
        # LST_COL_DATETIME
        if "LST_COL_DATETIME" in DIC_RAGIC_TABLE_CONF[table_id]:
            self.LST_COL_DATETIME = DIC_RAGIC_TABLE_CONF[table_id]["LST_COL_DATETIME"]
        else:
            self.LST_COL_DATETIME = []
        
        if "LST_COL_INT" in DIC_RAGIC_TABLE_CONF[table_id]:
            self.LST_COL_INT = DIC_RAGIC_TABLE_CONF[table_id]["LST_COL_INT"]
        else:
            self.LST_COL_INT = []
        
        if "LST_COL_BOOL" in DIC_RAGIC_TABLE_CONF[table_id]:
            self.LST_COL_BOOL = DIC_RAGIC_TABLE_CONF[table_id]["LST_COL_BOOL"]
        else:
            self.LST_COL_BOOL = []
        
        if "LST_COL_STR" in DIC_RAGIC_TABLE_CONF[table_id]:
            self.LST_COL_STR = DIC_RAGIC_TABLE_CONF[table_id]["LST_COL_STR"]
        else:
            self.LST_COL_STR = []

        if "LST_COL_WYSIWYG" in DIC_RAGIC_TABLE_CONF[table_id]:
            self.LST_COL_WYSIWYG = DIC_RAGIC_TABLE_CONF[table_id]["LST_COL_WYSIWYG"]
        else:
            self.LST_COL_WYSIWYG = []
        
        if "LST_COL_HTML" in DIC_RAGIC_TABLE_CONF[table_id]:
            self.LST_COL_HTML = DIC_RAGIC_TABLE_CONF[table_id]["LST_COL_HTML"]
        else:
            self.LST_COL_HTML = []

        self.ragic_file_api = f"{host}/sims/file.jsp?a={account}&f="
        self.dic_header_json = {
            'Content-type': 'application/json',
            'Accept': 'text/plain',
            # 'Content-type': 'multipart/form-data', # 這個加了反而不會成功 
            'Authorization': f'Basic {str_ragicAPIKey}'
            }
        self.verifySSL=dic_api["verifySSL"]
        self.use_cache=dic_api["use_cache"]
        self.force_update_cache=dic_api["force_update_cache"]
        self.lazy_update_cache=dic_api["lazy_update_cache"]
        self.intLazyCacheRefreshThreshold=dic_api["intLazyCacheRefreshThreshold"]

    def get_api_url(self,
                    # str_url:str,
                    str_query=""):
        """
        加上ragic key以及查詢參數
        """
        str_url = f"{self.str_url}?v=3&api&APIKey={self.str_ragicAPIKey}"
        if str_query != "":
            if str_query[0] == "&":
                str_url = f"{str_url}{str_query}"
            else: 
                str_url = f"{str_url}&{str_query}"

        log.debug(f"get_api_url {str_url=}")
        return str_url

    def get_file_url(self,str_fname:str)->str:
        if str_fname != "":    
            str_file_api = self.ragic_file_api
            str_url = f"{str_file_api}{str_fname}"
            log.debug(f"get_file_url {str_url=}")
            return str_url
        else:
            return str_fname

    def check_contain_chinese(self,check_str:str): 
        """
        check_contain_chinese("as是")= True
        check_contain_chinese("是as")= True
        check_contain_chinese("as,;0") = False
        check_contain_chinese("是-as") = True
        """
        # 2E80-2FDF, 3400-4DBF, 4E00-9FFF
        # 所有漢字 \u4E00-\u9FFF
        for char in check_str:
            if '\u4e00' <= char <= '\u9fff':
                return True
        return False

    def etl_data_in_dic_raw(self,dic_data_raw:dict):
        """
        消除 None 數值
        """
        dic_data = {}
        for key in dic_data_raw:
            if not dic_data_raw[key] == None:
                dic_data[key] = dic_data_raw[key]
        return dic_data    

    def requests_get_n_times(self,
                            str_url:str,
                            max_trial:int=3,
                            verifySSL:bool=dic_api["verifySSL"],
                        )->dict:
        """
        
        執行fetch; 如果回傳為{}就再重複打, 最多嘗試max_trial次 \n
        TODO: 建立有過期機制的cache,減少發request次數
        :param str_url:發request的url
        :param max_trial:最大嘗試次數,預設為3; 有時候ragic回應會是空的, 多試2次
        :param verifySSL:是否驗證SSL,預設False        
        :return r: dic_r = requests.get().json() 
        """
        trial = 0
        isFail = True
        t_sleep = 0.3

        dic_r={}
        try:
                        
            while trial < max_trial and isFail == True:
                try:
                    r = requests.get(str_url, verify=verifySSL)
                    dic_r = r.json()
                    if dic_r == {} or dic_r['status'] != 'SUCCESS':
                        isFail = True
                        time.sleep(t_sleep)
                    else:
                        isFail = False
                except:
                    isFail = True
                    time.sleep(t_sleep)
                finally:
                    trial += 1

        except Exception as e:            
            log.error(f"requests_get_n_times fail {str_url=} error:{e}")            

        return dic_r


    def get_lst_dic_direct_fetch(self,
                            str_url:str="",
                            verifySSL=dic_api["verifySSL"],     
                            # use_cache:bool=dic_api["use_cache"],
                            # force_update_cache:bool=dic_api["force_update_cache"],
                            # lazy_update_cache:bool=dic_api["lazy_update_cache"],                                          
                            )->list:
        
        
        if str_url == "":
            str_url = self.get_api_url(str_url="")

        max_trial = 3

        dic_r = self.requests_get_n_times(str_url,
                                        max_trial=max_trial,
                                        verifySSL=verifySSL,
                                        # use_cache=use_cache,
                                        # force_update_cache=force_update_cache,
                                        # lazy_update_cache=lazy_update_cache
                                        )
        #如果沒有資料會回傳 {}
        if dic_r == {}:
            log.debug(f"get_lst_dic_direct_fetch no data with {str_url=}")
            return []
        else:    
            try:
                lst_dic_r = list(dic_r.values())
                if lst_dic_r[0] != {}:
                    return lst_dic_r
                else:
                    return []
            except:
                return []


    def get_lst_urls_offset_limit(self,
                                str_url="", 
                                int_init_offset=0,
                                int_page_size=9999999,
                                int_max_id=10000):
        """
        列出offset和只應數量範圍
        :param api_url: 基礎API url字串
        :param int_init_offset:間隔
        :param int_page_size: 每一包數量 default=7000,
        :param int_max_id: 最大數量 default=7000
        :return: list of urls
            https:///<ragic_server_url>/<username>/<folder_name>/<sheet_name>?api&APIKey=<api_key>&limit={str_this_offset},{int_page_size}

        """
        if str_url == "":
            str_url = self.get_api_url()
        lst_offset = []
        lst_offset = range(int_init_offset,int_max_id,int_page_size)
        lst_url = []
        # lst_url = [lst_url.append(f"{API_URL}&limit={str_this_offset},{int_page_size}") for str_this_offset in lst_offset ]
        for str_this_offset in lst_offset:
            str_this_url = f"{str_url}&limit={str_this_offset},{int_page_size}"
            lst_url.append(str_this_url)
        
        return lst_url


    async def get_df_direct_fetch(self,
                            str_url:str="")->pd.DataFrame:
        if str_url == "":
            str_url = self.get_api_url()                
        lst_dic_r = await self.get_lst_dic_direct_fetch(str_url,verifySSL=dic_api["verifySSL"])
        df =  pd.DataFrame(lst_dic_r)
        return df


    def get_df_all_rows_ragic(self,
                            str_url:str="",
                            intoffset=0,
                            intpage=9999999)->pd.DataFrame:
        """
        取得ragic table中所有資料
        由API網址加上limit=0,0以取得所有資料(不包含APIKey的疊加,請傳入時就加上去)
        """
        if str_url == "":
            str_url = self.get_api_url()
        str_url = f"{str_url}&limit={intoffset},{intpage}"       
        df = self.get_df_direct_fetch(str_url)    
        return df

        # 建立查詢where字串
    def get_str_regex(self,
                    str_query):
        """
        同欄位的多選需要用regex (含去空格)
        判斷: split to list, loop list, 建立regex字串
        :param str_query: "|"區隔關鍵字字串
            e.g. status=Angel|Seed|A str_status = "Angel|Seed|A" 
        :return: |區隔regex or 字串, e.g. "天使輪|A輪|B輪"
        """
        if str_query != "":
            try:
                lst_query = str(str_query).split("|")
                lst_query = [str_this.strip() for  str_this in lst_query]
                str_regex = "|".join(lst_query)
            except:
                str_regex = ""
        else:
            str_regex = ""        

        return str_regex

    def convert_dic_query_to_str_query(self,
                                    dic_query:dict={},                                    
                                    ):
        """
        將dic_query轉成str_query
        :param dic_query:
            {
                "colname":{
                "operator": regex / eq ,
                "value": str / int / float / bool / datetime
                },...
            }
        :return:
        """
        # dic_tb_col_id = DIC_RAGIC_TABLE_CONF[str_ragic_id]["DIC_COL_ID"]
        dic_tb_col_id = self.dic_tb_col_id

        str_query = ""
        for key, value in dic_query.items():
            if key in dic_tb_col_id and key != "limit" and key != "offset":
                str_col_id = dic_tb_col_id[key]
                str_operator = value["operator"]
                str_value = value["value"]
                str_query += f"&where={str_col_id},{str_operator},{str_value}"

            else:
                if key != "limit" and key != "offset":
                    log.warning(f"{key} not in dic_tb_col_id")

        if "offset" in dic_query or "limit" in dic_query:
            if "limit" in dic_query:
                str_limit = dic_query["limit"]["value"]
            else:
                str_limit = "99999999"
            
            if "offset" in dic_query:
                str_offset = dic_query["offset"]["value"]
            else:
                str_offset = 0

            str_query += f"&limit={str_offset},{str_limit}"
        


        return str_query


    async def request_post_n_times(self,
                                str_api_url, 
                                dic_data, 
                                dic_header,
                                max_trial=5,
                                )->dict:
        """
        嘗試n次post 如果回傳為{}就再重複打, 最多嘗試max_trial次
        :param str_api_url: e.g. https://ap5.ragic.com/2021ASVDA/-0503/20/698?v=3&api                
        :param dic_data: 要打上去的資料
        :param dic_header: 如{'Content-type': 'application/json', 'Accept': 'text/plain'}
        :param max_trial: 最大嘗試次數
        :return dict: dict(r.json()["data"])
        """    
        trial = 0
        # max_trial = 3
        isFail = True
        t_sleep = 0.5
        lst_dic_r=[]
        while trial < max_trial and isFail == True:
            try:
                
                    #  verify='/path/to/public_key.pem'
                
                r = requests.post(str_api_url,
                                data=dic_data,
                                headers=dic_header,
                                verify=dic_api["verifySSL"])
                            
                dic_r = r.json()   
                if dic_r == {} or dic_r['status'] != 'SUCCESS':
                    isFail = True                
                    time.sleep(t_sleep)
                else:
                    isFail = False      
                                
            except:
                dic_r={}
                isFail = True
                time.sleep(t_sleep)
            finally:
                trial += 1  
                
        if isFail:
            log.debug(f"request_post_n_times no data with {str_api_url=}")
            return {}
            
        else:
            dic_r_json = dict(r.json()["data"])
            return dic_r_json


    async def ragic_request_post_n_times(self,
                                        lst_dic_update,
                                        i,
                                        # ragic_tab_url,
                                        # ragic_table_id,
                                        dic_header
                                        ):
        # int_record_id = int_ragic_data_offset + i  
        ragic_tab_url = self.ragic_tab_url
        ragic_table_id = self.table_id
        _int_record_id = lst_dic_update[i]["_ragicId"]
        _str_api_url = f"{ragic_tab_url}/{ragic_table_id}/{_int_record_id}?v=3&api"
        # print(str_api_url)
        # print(i,":")
        _dic_data = lst_dic_update[i]    
        # print(dic_data)
        try:
            dic_r_json = await self.request_post_n_times(_str_api_url, _dic_data, dic_header)
            return dic_r_json
        except Exception as e:
            # print(e)
            log.debug(f"ragic_request_post_n_times no data with {_str_api_url=} {e=}")
            return {}
        
    async def batch_ragic_request_post_n_times(self,
                                            lst_dic_update,
                                            # ragic_tab_url,
                                            # ragic_table_id,
                                            dic_header,
                                            method=2):
        ragic_tab_url = self.ragic_tab_url
        ragic_table_id = self.table_id                                            
        
        if method == 1:
            lst_result = []   
            for i in range(len(lst_dic_update)):  
                # int_record_id = int_ragic_data_offset + i 
                _int_record_id = lst_dic_update[i]["_ragicId"] 
                _str_api_url = f"{ragic_tab_url}/{ragic_table_id}/{_int_record_id}?v=3&api"
                # print(str_api_url)
                # print(i,":")
                dic_data = lst_dic_update[i]
                # print(dic_data)
                try:
                    dic_r_json = await self.request_post_n_times(_str_api_url, dic_data, dic_header)
                except Exception as e:
                    print(e)
                lst_result.append(dic_r_json)

        elif method == 2:
            try:
                task = [asyncio.create_task(self.ragic_request_post_n_times(lst_dic_update,
                                                                        i,
                                                                        # ragic_tab_url,
                                                                        # ragic_table_id,
                                                                        dic_header)) 
                                                                        for i in range(len(lst_dic_update))]
                await asyncio.wait(task, timeout=8)
            except Exception as e:
                print(e)
            lst_result = []   
            for t in task:
                try:
                    r = t.result()
                except Exception as e:
                    # lst_result.append(e)
                    print(e)
                    lst_result.append({})
                else:
                    lst_result.append(r)

        return lst_result

    async def ragic_request_create_n_times(self,
                                        lst_dic_update:list,
                                        i:int,
                                        # ragic_tab_url,
                                        # ragic_table_id,
                                        dic_header:dict,
                                        data_in_col_is_colid:bool=True
                                    ):
        ragic_tab_url = self.ragic_tab_url
        ragic_table_id = self.table_id
        str_api_url = f"{ragic_tab_url}/{ragic_table_id}/?v=3&api"
        # print(str_api_url)
        # print(i,":")
        dic_data = lst_dic_update[i]
        # print(dic_data)
        # 轉換成colid
        dic_data_update={}

        if not data_in_col_is_colid:
            # 轉換成colid
            for col in dic_data:                
                if col in self.dic_tb_col_id:
                    _str_col_id = self.dic_tb_col_id[col]
                    dic_data_update[_str_col_id] = dic_data[col]
            
        else:
            dic_data_update = copy.deepcopy(dic_data)

        try:
            dic_r_json = await self.request_post_n_times(str_api_url, 
                                                        dic_data_update, 
                                                        dic_header
                                                    )
            return dic_r_json
        except Exception as e:
            # print(e)
            log.debug(f"ragic_request_create_n_times no data with {str_api_url=} {e=}")
            return {}
        

    async def batch_ragic_request_create_n_times(self,
                                            lst_dic_update,
                                            # ragic_tab_url,                                        
                                            # ragic_table_id
                                            ):    
        """
        批次將資料post進ragic
        :param lst_dic_update: 已將欄位轉換成column id 的資料
        :param ragic_tab_url: 包含https://www.ragic.com/<account>/<tab folder>/ 的網址
        :param ragic_table_id: <sheet index>
        """
        
        dic_header = {'Content-type': 'application/json', 'Accept': 'text/plain'}
        dic_header["Authorization"] = f"Basic {RagicAPIKey}"    
        try:
            task = [asyncio.create_task(self.ragic_request_create_n_times(lst_dic_update,
                                                                    i,
                                                                    # ragic_tab_url,
                                                                    # ragic_table_id,
                                                                    dic_header)) for i in range(len(lst_dic_update))]
            await asyncio.wait(task, timeout=8)

        except Exception as e:
            print(e)

        lst_result = []   
        for t in task:
            try:
                r = t.result()
            except Exception as e:
                # lst_result.append(e)
                lst_result.append({})
            else:
                lst_result.append(r)

        return lst_result

    async def request_delete_n_times(self,
                                    str_api_url, 
                                    dic_header, 
                                    max_trial=3):
        """
        嘗試n次post 如果回傳為{}就再重複打, 最多嘗試max_trial次 \n
        :param str_api_url \n
        :param dic_header: 如{'Content-type': 'application/json', 
            'Accept': 'text/plain', 'Authorization':'Basic xxxtoken'}
        :param max_trial: 最大嘗試次數 \n
        :return dict: dict(r.json()["data"])
        """
        trial = 0
        # max_trial = 3
        isFail = True
        t_sleep = 0.5
        lst_dic_r=[]
        while trial < max_trial and isFail == True:
            try:
                r = requests.delete(str_api_url, headers=dic_header)
                # dic_r = r.json()   
                """
                r.json() = {
                'msg':'The record has been moved to recycle bin.'
                'ragicId':2815}

                """
                # if dic_r == {} or dic_r['status'] != 'SUCCESS':
                if r.status_code == 200:                
                    isFail = False      
                else:                                    
                    isFail = True  
                    time.sleep(t_sleep)

            except:                
                r=False
                isFail = True
                time.sleep(t_sleep)

            finally:
                trial += 1  
                
        return r
        
    async def delete_ragic_table(self,
                                # ragic_id,
                                int_record_id,
                                str_ragic_api_key
                            ):
        """
        ref: https://www.ragic.com/intl/zh-TW/doc-api/20/Deleting-an-entry
        """
        ragic_id = self.table_id
        ragic_tab_url = self.ragic_tab_url
        # str_dt_now = get_str_now_dt()
        # dic_data["updateTime"] = str_dt_now
        dic_header = {'Content-type': 'application/json', 'Accept': 'text/plain'}
        dic_header["Authorization"] = f"Basic {str_ragic_api_key}"
        # str_api_url = f"{RAGIC_TAB_URL}/{ragic_id}/{int_record_id}?api&APIKey={str_ragic_api_key}"
        str_api_url = f"{ragic_tab_url}/{ragic_id}/{int_record_id}?api"
        r = await self.request_delete_n_times(str_api_url,dic_header)
        #ETL    
        if r.status_code == 200:
            return r
        else:   
            str_msg =  f"ragic_id {ragic_id}, record id {int_record_id} 刪除失敗。"
            log.warning(str_msg)
            # raise BaseException("刪除失敗。") 
            return False

    async def batch_request_delete_n_times(self,
                                            lst_ragicid,
                                            i,
                                            # ragic_tab_url,
                                            # ragic_table_id,
                                            dic_header):
        ragic_tab_url = self.ragic_tab_url
        ragic_table_id = self.table_id
        str_record_id = str(lst_ragicid[i])
        str_api_url = f"{ragic_tab_url}/{ragic_table_id}/{str_record_id}?api"
        # print(str_api_url)
        # print(i,":")        
        # print(dic_data)
        try:
            dic_r_json = await self.request_delete_n_times(str_api_url, 
                                                        dic_header, 
                                                        max_trial=3)
            return dic_r_json
        except Exception as e:
            # print(e)
            log.warning(f"batch_request_delete_n_times no data with {str_api_url=} {e=}")
            return {}
        

    async def batch_ragic_delete_n_times(self,
                                        lst_ragicid,
                                        # ragic_tab_url,                                        
                                        # ragic_table_id,
                                        method=2                                     
                                        ):
        # ragic_tab_url = self.RAGIC_TAB_URL
        # ragic_table_id = self.table_id
        str_ragic_api_key = RagicAPIKey
        lst_result = []   
        dic_header = {'Content-type': 'application/json', 'Accept': 'text/plain'}
        dic_header["Authorization"] = f"Basic {str_ragic_api_key}"
        if method == 1:
            for i in range(len(lst_ragicid)):  
                dic_r_json = await self.batch_request_delete_n_times(lst_ragicid,
                                                                    i,
                                                                    # ragic_tab_url,ragic_table_id,
                                                                    dic_header)
                lst_result.append(dic_r_json)    
            
        elif method == 2:
            lst_result = []   
            try:
                task = [asyncio.create_task(self.batch_request_delete_n_times(lst_ragicid,
                                                                            i,
                                                                            # ragic_tab_url,ragic_table_id,
                                                                            dic_header)
                                                                            ) for i in range(len(lst_ragicid))]
                await asyncio.wait(task, timeout=8)
                
                for t in task:
                    try:
                        r = t.result()
                    except Exception as e:
                        # lst_result.append(e)
                        lst_result.append({})
                    else:
                        lst_result.append(r)
                        
            except Exception as e:
                # print(e)
                str_msg = f"batch_ragic_delete_n_times failed {e}"
                log.warning(str_msg)

        return lst_result

    def lst_dic_to_csv(self,
                    lst_dic:dict,
                    path_fname:Path,
                    compression_type:str="infer"):
        """
        將list of dictionary透過pandas dataframe另存成csv的壓縮檔案bz2, gzip, or zip
        :param lst_dic: 可轉為dataframe的list of dictionary
        :param path_fname: pathlib.Path 物件
        :param compression_type: default:"infer"; 其他可用:"bz2", "gzip", or "zip"
        :return path_fname: 檔案名稱pathlib.Path object
        """
        df = pd.DataFrame(lst_dic)
        # if compression == None:
        #     df.to_csv(pth_fname,index=False)
        # else:
        df.to_csv(path_fname,index=False, compression=compression_type)
        str_msg = f"{path_fname} saved."
        # print(f"{path_fname} saved.")
        log.info(str_msg)
        return path_fname

    def if_str_in_colname_exists(self,
                                str_query, 
                                # dic_tb_col_id, 
                                str_colname, 
                                str_url, 
                                is_colname_api=True
                            ):
        # get
        # dic_tb_col_id = DIC_USER_COL_ID
        dic_tb_col_id = self.dic_tb_col_id
        str_id_col = dic_tb_col_id[str_colname]
        str_url = f'{str_url}&where={str_id_col},eq,{str_query}'
        lst_dic = self.get_lst_dic_direct_fetch(str_url,verifySSL=dic_api["verifySSL"])
        #不需要強制修改欄位名稱, 因為dic_map與config中對照表上採用的是api colname
        # if not is_colname_api:
            # dic_map_api = DIC_RENAME_COMPANY_RAGIC_API
            # for i in range(len(lst_dic)):
            # # 欄位名稱換成API用的名稱
            #     if is_colname_api:
            #         try:
            #             lst_dic[i] = dict_col_rename_mapping(lst_dic[i],dic_map_api)        
            #         except:
            #             pass
        if len(lst_dic) >= 1:
            return True
        else:
            return False



    def get_url_fts(self,
                    api_url:str, 
                    str_fts:str):
        """
        關鍵字全文(所有欄位)檢索
        :param api_url: 基礎API url字串
        :param str_fts: 關鍵字串
        :param lst_dic_cols_query: [{
                "str_col_name":str, #status
                "str_query":str #"天使輪 ,A輪 , B輪"
            },..
            ]
        :return lst_url: e.g. https:///<ragic_server_url>/<username>/<folder_name>/<sheet_name>?api&APIKey=<api_key>&fts=<str_fts>
        """
        # lst_url = []
    #     if str_fts != None:
    #         str_fts = str_fts.replace(" ",",")
        try:
            str_api_url = f"{api_url}&fts={str_fts}"
            if "APIKey" not in str_api_url:
                str_api_url = f"{str_api_url}&APIKey={RagicAPIKey}"
            return str_api_url
        # lst_url.append(str_api_url)
        # return lst_url
        
        except Exception as e:
            # print(e)
            str_msg = f"get_url_fts failed {e}"
            log.warning(str_msg)
            
    def etl_drop_dic_ragic_cols(self,
                            dic_this                            
                        ):

        # global LST_DROP_RAGIC_COLS
        lst_drop_ragic_cols = LST_DROP_RAGIC_COLS
        dic_res = copy.deepcopy(dic_this)
        for key_to_be_deleted in lst_drop_ragic_cols:
            try:
                dic_res.pop(key_to_be_deleted, None)
            except:
                pass
            try:
                dic_res.pop("_index_calDates_",None)
            except:
                    pass
        
        return dic_res

    def etl_dic_cols(self,
                    dic_this, 
                    lst_drop_ragic_cols= LST_DROP_RAGIC_COLS):
        # global LST_DROP_RAGIC_COLS
        # lst_drop_ragic_cols = LST_DROP_RAGIC_COLS
        dic_ret = copy.deepcopy(dic_this)
        for key_to_be_deleted in lst_drop_ragic_cols:
            try:
                dic_ret.pop(key_to_be_deleted, None)
            except:
                pass
            try:
                dic_ret.pop("_index_calDates_",None)
            except:
                    pass
        
        return dic_ret


    def etl_lst_dic_cols(self,lst_dic):
        """
        移除特定欄位["_ragicId","_star","_index_title_","_index_","_seq"]
        """
        global LST_DROP_RAGIC_COLS
        lst_drop_ragic_cols = LST_DROP_RAGIC_COLS
        lst_dic_res = []
        for this_dic in lst_dic:
            this_dic = self.etl_dic_cols(this_dic, lst_drop_ragic_cols)

            lst_dic_res.append(this_dic)
        
        return lst_dic_res

                
    def etl_df_ragic_cols(self,df):
        """
        移除df中ragic特有欄位
        ["_ragicId","_star","_index_title_","_index_","_seq"]
        用public api request跟直接輸入api, 取得結果不同, 會少於ragic
        """
        global LST_DROP_RAGIC_COLS
        lst_drop_ragic_cols_default = LST_DROP_RAGIC_COLS
        # try:
        #     df = df.fillna("")
        # except:
        #     pass
        
        lst_drop_ragic_cols = []
        for col in df.columns:
            if col in lst_drop_ragic_cols_default:
                lst_drop_ragic_cols.append(col)

        try:            
            df.drop(labels=lst_drop_ragic_cols, axis=1, inplace=True)
        except Exception as e:
            # print(e)
            row1col1 = str(df.iloc[0,0])
            row1col2 = str(df.iloc[0,1])
            str_msg = f"{row1col1} {row1col2}"
            log.warning(f"etl_df_ragic_cols {str_msg} error ")

        finally:
            return df


    def etl_ragic_api_response(self,dic_r_raw, 
                            # dic_tb_col_id,
                            preserveRagicCols=False):
        """
        post/delete後取得的response的清理
        :param dic_r_json: 
        """
        dic_tb_col_id = self.dic_tb_col_id
        
        if not preserveRagicCols:
            lst_drop_ragic_cols = LST_DROP_RAGIC_COLS
            dic_r_raw = self.etl_dic_cols(dic_r_raw, lst_drop_ragic_cols)
        
        # 對應回原本的欄位名稱    
        dic_tb_col_id_inv = inv_dict_mapping(dic_tb_col_id)
        dic_r_etl = dict_col_rename_mapping(dic_r_raw, dic_tb_col_id_inv)

        # 將英文api name對應回中文欄位名稱, 應該要在各自的model下面做
        if "id" in dic_r_raw:
            dic_r_etl['id'] = int(dic_r_raw['id'] )

        if "_ragicId" in dic_r_raw:
            dic_r_etl['_ragicId'] = int(dic_r_raw['_ragicId'] )
            
        return dic_r_etl

    def etl_ragic_wysiwyg_value(self,str_value):
        """
        Focus on emerging market <enter>[br][/br]
        Find future rising star[br][/br]
        [b]bold[/b][br][/br]
        [i]italic[/i][br][/br]
        [u]underscore[/u][br][/br]
        
        TODO:
        [color=rgb(51, 102, 255)]color blue[/color][br][/br]
        [size=20px]
        [font=微軟正黑體, sans-serif]font type 微軟正黑22[/font]
        [/size][br][/br]
        [br][/br]
        """
        # replace [br][/br] to <br>
        str_value = str_value.replace("[br][/br]","<br>")
        # replace [b] to <b>; [/b] to </b>
        # str_value = str_value.replace("[b]","<b>")
        # str_value = str_value.replace("[/b]","</b>")
        # replace [u] to <u>; [/u] to </u>
        str_value = str_value.replace("[u]","<u>")
        str_value = str_value.replace("[/u]","</u>")
        # replace [i] to <i>; [/i] to </i>
        str_value = str_value.replace("[i]","<i>")
        str_value = str_value.replace("[/i]","</i>")
        # replace [i] to <i>; [/i] to </i>
        # replace [font]to <font>; [/font] to </font>
        # str_value = str_value.replace("[/font]","</font>")

        # <span style="color: rgb(0, 0, 0);"> to <span style="color: #000000;">

        # str_value = str_value.replace("[font=","<font")

        
        return str_value

    def etl_ragic_wysiwyg(self, dic_this:dict):
        """

        """
        dic_ret = {}
        for key, value in dic_this.items():
            if key in self.LST_COL_WYSIWYG:
                dic_ret[key] = self.etl_ragic_wysiwyg_value(value)
            else:
                dic_ret[key] = value

        return dic_ret


    def etl_ragic_api_response_lst(self,
                                lst_dic_raw:list,                                
                                str_date_fmt_in:str=INPUT_DATE_FMT,
                                str_date_fmt_out:str=OUTPOUT_DATE_FMT,
                                # use_cache:bool=dic_api["use_cache"],
                                # force_update_cache:bool=dic_api["force_update_cache"],
                                # lazy_update_cache:bool=dic_api["lazy_update_cache"],                                
                                etl_col_file:bool=True,
                                etl_col_multi:bool=True,
                                etl_col_date:bool=True,
                                etl_col_int:bool=True,
                                etl_col_wysiwyg:bool=True,
                                etl_col_html:bool=True,
                                etlRagicCols:bool=True,                                                                
                                # etl_col_float:bool=True,                                
                            ):
        """
        處理etl_col_file, etl_col_multi,etl_col_datetime,etl_col_int,
            etl_ragic_wysiwyg,etl_col_html
        :param lst_dic_raw:待轉換的原始list of dict        
        :param str_date_fmt_in: input資料採用的datetime 格式字串
        :param str_date_fmt_out: outpur資料採用的datetime 格式字串
        :param use_cache: html檔案是否使用cache
        :param force_update_cache: html檔案是否強制更新cache
        :param lazy_update_cache: html檔案是否延遲更新cache
        :param etl_col_file: 是否要處理file欄位
        :param etl_col_multi: 是否要處理multi欄位
        :param etl_col_date: 是否要處理date欄位
        :param etl_col_int: 是否要處理int欄位
        :param etl_col_wysiwyg: 是否要處理wysiwyg欄位
        :param etl_col_html: 是否要處理html欄位
        :param etlRagicCols:是否要移除ragic既有的欄位
        :return: 
        """
        # lst_dic_etl = copy.deepcopy(lst_dic_r_raw)
        lst_dic_etl = []

        if lst_dic_raw == [] or lst_dic_raw == None:
            return lst_dic_raw

        for dic in lst_dic_raw:    
            try:
                dic_etl = copy.deepcopy(dic)    
                if etl_col_file:
                    dic_etl = self.etl_col_file(dic_etl)
                
                if etl_col_multi:
                    dic_etl = self.etl_col_multi(dic_etl)
                
                if etl_col_date:
                    if str_date_fmt_in != str_date_fmt_out:
                        dic_etl = self.etl_col_datetime(dic_etl,
                                        str_fmt_in=str_date_fmt_in, #"%Y/%m/%d",
                                        str_fmt_out=str_date_fmt_out, #"%Y-%m-%d"
                                    )
                
                if etl_col_int:
                    dic_etl = self.etl_col_int(dic_etl)
                
                if etl_col_wysiwyg:
                    dic_etl = self.etl_ragic_wysiwyg(dic_etl)

                if etl_col_file and etl_col_html:
                    dic_etl = self.etl_col_html(dic_etl,
                                        verifySSL=dic_api["verifySSL"],
                                        # use_cache=use_cache,
                                        # force_update_cache=force_update_cache,
                                        # lazy_update_cache=lazy_update_cache
                                    )
                
                if etlRagicCols:
                    dic_etl = self.etl_drop_dic_ragic_cols(dic_etl)

                lst_dic_etl.append(dic_etl)

            except Exception as e:
                log.error(f"etl_ragic_api_response_lst {e}")
        
        return lst_dic_etl


    async def ragic_request_update_id_value(self,
                                    dic_params,
                                    # ragic_id,
                                    int_record_id,
                                    # str_ragic_api_key
                                    ):
        """
        將ragic id欄位與ragic id row的資料,透過ragic api更新上特定ragic table
        :param dic_params: 帶有ragic id欄位名稱的資料        
        :param int_record_id: 要更新的資料ragic id        
        :return 
        """
        # dic_map = DIC_USER_COL_ID
        ragic_id = self.table_id
        ragic_tab_url = self.ragic_tab_url
        str_ragic_api_key = RagicAPIKey
        dic_header = {'Content-type': 'application/json', 'Accept': 'text/plain'}
        dic_header["Authorization"] = f"Basic {str_ragic_api_key}"        
        str_api_url = f"{ragic_tab_url}/{ragic_id}/{int_record_id}?api"
        
        dic_r_json = await self.request_post_n_times(str_api_url, 
                                                    dic_params, 
                                                    dic_header)
        
        return dic_r_json


    async def create_or_update_article_meta_ragic(self,
                                    dic_data_in:dict,
                                    str_unique_col_name:str,
                                    data_col_is_colid:bool=False
                                    )->dict:
        """
        :param dic_data={
            str_unique_col_name,str_unique_col_value,
            colname:colvalue,...
            }
        :param str_unique_col_name: 唯一欄位名稱
        :param data_col_is_colid: dic_data是否為採用column id為key的dict, 預設為False
        :return: dict
        """
        # dic_tb_col_id = DIC_ARTICLE_COL_ID
        # dic_tb_col_id = DIC_RAGIC_TABLE_CONF[str(RAGIC_ARTICLE_ID)]["DIC_COL_ID"]
        # str_api_url = article_Ragic_API_public
        # dic_data = copy.deepcopy(dic_data_in)
        if not data_col_is_colid:
            dic_params = dict_col_rename_mapping(dic_data_in, dic_tb_col_id)
        else:
            dic_params = copy.deepcopy(dic_data_in)

        dt_now = datetime.now() # current date and time
        # str_now = dt_now.strftime("%Y/%m/%d %H:%M:%S")
        # doExist = await if_ragic_article_name_exists(dic_data["filename"])
        """
            str_api_url = article_Ragic_API_public
           isExists = if_str_in_colname_exists(str_query, 
                                    dic_tb_col_id, 
                                    str_colname, 
                                    str_api_url)
        """
        # str_api_url=self.str_url
        dic_tb_col_id = self.dic_tb_col_id
        # str_id_col = dic_tb_col_id[str_unique_col_name]
        # str_unique_col_val = dic_data[str_unique_col_name]
        # str_url = f'{str_url}&where={str_id_col},eq,{str_unique_col_val}'
        dic_query = {
            str_unique_col_name: {
                "operator":"eq",
                "value":dic_data_in[str_unique_col_name]
            }                                
        }
        str_query_url = self.get_api_url(dic_query)
        lst_dic_raw = self.get_lst_dic_direct_fetch(
                                            str_query_url,
                                            verifySSL=dic_api["verifySSL"]
                                            )
        if len(lst_dic_raw) >= 1:
            doExist= True
        else:
            doExist= False
        # doExist = self.if_str_in_colname_exists(dic_data[str_unique_col_name],
        #                             str_unique_col_name,                                    
        #                             self.str_url,
        #                             )

        # from datetime import datetime
        dic_header = self.dic_header_json
        """
        dic_header={
            'Content-type': 'application/json',
            # 'Content-type': 'multipart/form-data', # 這個加了反而不會成功 
            'Authorization': f'Basic {str_ragicAPIKey}'
        }
        """
        if not doExist:
            # 建立資料不需要 _ragicId                    
            lst_dic_update = [dic_params]            
            # 建立資料
            dic_r = await self.ragic_request_create_n_times(lst_dic_update,0,dic_header)

        else: #更新資料
            
            # 確認是否 _ragicId存在於data_raw,如果不存在就要先取得, 然後把_ragicId更新到data_raw
            if "_ragicId" not in dic_data_in:
                # int_ragicId = dic_raw["_ragicId"]
                int_ragicId =  lst_dic_raw[0]["_ragicId"]
                
                str_ragicId = str(int_ragicId)
                # dic_data["_ragicId"] = str_ragicId
            else:
                str_ragicId = str(dic_data_in["_ragicId"])
            #更新資料ragic_post_update_id_value
            dic_r = await self.ragic_request_update_id_value(dic_params,
                                                    str_ragicId
                                                    )
        # 對回應做 ETL: 
        try:
            dic_r_etl = self.etl_ragic_api_response(dic_r,preserveRagicCols=True)
        except:
            dic_r_etl = dic_r
   
        return dic_r_etl

    async def upload_files_ragic(self,                            
                                str_filename:str,                            
                                fpath:Path,
                                str_record_id:str, 
                                str_col_id:str,                                    
                                # update_cache:bool=True    # TODO                                            
                                # ragic_tab_id:str
                                ):
        """        
        ref: 
        https://www.ragic.com/intl/zh-TW/doc-api/29/Upload-files-and-images
        get:
        "1000148": "r2EQGsw5D8@uploadtest.html"
        https://www.ragic.com/sims/file.jsp?a=demo&f=Ni92W2luv@My_Picture.jpg

        multipart/form-data \n
        :param str_filename: 檔案名稱 \n
        :param fpath:Path 要上傳的檔案路徑 \n
        :param ragic_tab_id: ragic資料表的id \n
        :param str_record_id: table中該筆資料的record id NOTE ragic文件中沒有這個, 可能要移除掉 \n
        (xx :param str_colname: 欄位名稱, 用來查詢ragic column id) \n
        :parm str_col_id: = dic_detail_col_id[str_colname] \n
        (xx :param dic_detail_col_id: column name跟ragic column id 對照e.g. DIC_ARTICLE_COL_ID) \n
        :return r: requests.response \n
        ## json.loads(r.text)["status"] = 'SUCCESS'
        ## retrieve filename: json.loads(r.text)["data"][str_col_id] = 5fY6J29gK1@uploadtest2.html \n
        """

        str_ragic_api_key = self.str_ragicAPIKey
        str_filename = str_filename.encode('utf-8')

        # if str_record_id != "":    
        str_api_url = f"{self.ragic_tab_url}/{self.table_id}/{str_record_id}"
        # else:
        #     str_api_url = f"{RAGIC_TAB_URL}/{ragic_tab_id}"
        
        str_fpath = str(fpath)

        # content-type is multipart/form-data
        dic_header = {
            # 'Content-type': 'multipart/form-data', # 這個加了反而不會成功 
            'Authorization': f'Basic {str_ragic_api_key}',
            "filename":str_filename #因為此欄位必填
            }   
        try:
            with open(str_fpath,'r',encoding='UTF-8') as _file:
                dic_files = {
                    str_col_id: (
                        str_filename, 
                        # open(str_fpath,'r',encoding='UTF-8')
                        _file
                    )
                }
                r = requests.post(str_api_url,                                 
                                files=dic_files, 
                                headers=dic_header,
                                verify=dic_api["verifySSL"]
                                )

            return r
            
        except Exception as e:
            # print(e)
            str_msg = f"upload_files_ragic failed {e}"
            log.warning(str_msg)

    def etl_col_file(self,dic:dict)->dict:
        lst_col_file=self.LST_COL_FILE
        dic_ret = {}
        """將所有|分隔的欄位的字串都變成list"""
        for col in dic:            
            if col in lst_col_file:            
                try:
                    if dic[col] != "":
                        dic_ret[col] = self.get_file_url(dic[col])
                    else:
                        dic_ret[col] = ""
                except Exception as e:
                    print(e)
                    dic_ret[col] = dic[col]
            else:
                dic_ret[col] = dic[col]

        return dic_ret

    def read_html_file_to_str(
                    self,
                    fpath:Path
                    )->str:
        """
        自檔案名稱取得檔案路徑, 並且取得其內的字串 \n
        (整個檔案,包含HTML head body等, 未進行進一步parsing)
        """
        with open(fpath, "r", encoding='utf-8') as f:
            str_html = f.read()    
            str_html = str_html.replace("\r\n","").replace("\n","")

        return str_html

    def write_html_file(
                        self,
                        str_html:str,
                        fpath:Path
                        )->bool:
        """
        將字串寫入目標Path HTML檔案 
        (整個檔案,包含HTML head body等, 未進行進一步parsing)
        """
        try:
            with open(fpath, "w") as file:
                file.write(str_html)
            return True
        except:
            return False

    def get_str_from_html_url(self,
                            str_url:str,
                            max_trial:int=3,
                            verifySSL:bool=False
                        )->str:
        """
        request url n times and convert to str_html
        """
        if str_url == "":
            return ""
        
        trial = 0
        isFail = True
        t_sleep = 0.3
        while trial < max_trial and isFail == True:
            try:
                f = requests.get(str_url,verify=verifySSL)
                # contentType = 'text/html;charset=utf-8'
                if 'text/html' in f.headers['content-type']:
                    str_html = f.text.replace("\r\n","").replace("\n","")                    
                else: #'image/jpeg;charset=utf-8'
                    str_html = ""
                isFail = False

            except Exception as e:    
                str_html = ""
                isFail = True
                time.sleep(t_sleep)
        
        return str_html

    def etl_html_url_to_str(
                    self,
                    str_url:str,
                    # _max_trial:int=3,
                    verifySSL:bool=dic_api["verifySSL"],
                    )->str:
        """
        自網址取得字串, 並且取得其內的字串 \n
        (整個檔案,包含HTML head body等, 未進行進一步parsing)
        包含存取cache機制設定
        """
        isFail = True        
        _max_trial=3
        try:            
            str_html = ""
            
            str_html = self.get_str_from_html_url(str_url,
                                                max_trial=_max_trial,
                                                verifySSL=verifySSL
                                                    )                            
        except Exception as e:            
            log.error(f"etl_html_url_to_str fail {str_url=} error:{e}")
       
        return str_html


    def etl_col_html(self,dic:dict,
                    verifySSL:bool=dic_api["verifySSL"],
                    # use_cache:bool=dic_api["use_cache"],
                    # force_update_cache:bool=dic_api["force_update_cache"],
                    # lazy_update_cache:bool=dic_api["lazy_update_cache"]
                    )->dict:
        """
        輸入的欄位內容必須為url, 處理時會取得url內的html並輸出為string
        增加在欄位 "str_"+colname
        TODO: 加上cache機制
        """
        lst_col_html=self.LST_COL_HTML
        dic_ret = {}        
        for col in dic:            
            dic_ret[col] = dic[col]
            if col in lst_col_html:            
                col_new = "str_" + col
                try:
                    if dic[col] != "":
                        dic_ret[col_new] = self.etl_html_url_to_str(dic[col],
                                                            verifySSL=verifySSL,
                                                            # use_cache=use_cache,
                                                            # force_update_cache=force_update_cache,
                                                            # lazy_update_cache=lazy_update_cache
                                                            )
                    else:
                        dic_ret[col_new] = ""

                except Exception as e:
                    print(e)
                    dic_ret[col_new] = dic[col]

        return dic_ret

    def etl_col_media(self,
                    dic:dict,
                    to_get_file_url:bool=True)->str:
        """
        TODO: 尚未驗證
        將ragic的media欄位轉成網址
        去查詢media欄位中的name, 並且回傳file的檔案id, 最後轉成絕對網址
        """
        lst_col_media=self.LST_COL_MULTI
        dic_ret = copy.deepcopy(dic)
        for col in dic:            
            if col in lst_col_media: 
                
                # 查詢media table的id
                str_media_tab_id = self.dic_detail_tab_id["media"]
                media_table = RagicTable(host=self.RAGIC_HOST,
                                        account=RAGIC_ACCOUNT,
                                        project=RAGIC_PROJECT_NAME,
                                        table_id=str_media_tab_id,
                                        str_ragicAPIKey=RagicAPIKey)
                dic_query = {            
                    "name":{ "operator":"eq", "value":dic[col]},
                }
                str_query = media_table.convert_dic_query_to_str_query(dic_query)
                str_url = media_table.get_api_url(str_query)
                lst_dic = media_table.get_lst_dic_direct_fetch(str_url)
                if len(lst_dic) > 0:
                    dic_media = lst_dic[0]
                    dic_ret[col] = dic_media["file"]

                    if to_get_file_url:
                        dic_ret[col] = media_table.get_file_url(dic_ret[col])
                else:
                    dic_ret[col] = ""

        return dic_ret

    def etl_col_multi(self,
                    dic:dict,
                    # isList:bool=True,
                    toStr:bool=False,
                    sep:str="|")->dict:
        """將所有array變成字串 或所有|分隔的欄位的字串都變成list
        :param dic:輸入的dict
        :param isList:是否輸入的COL_MULTI為array/list, 預設True
        :param sep: 如果不是array,其分隔符號為何? 預設|
        :return:
        """
        lst_col_multi=self.LST_COL_MULTI
        dic_ret = copy.deepcopy(dic)
        
        for col in dic:            
            if col in lst_col_multi:            
                try:
                    if dic[col] != "" and dic[col] != []:
                        # dic_ret[col] = dic[col]
                        if toStr:
                            dic_ret[col] = ",".join(dic_ret[col]) 
                        else:                        
                            # dic_ret[col] = dic[col]
                            dic_ret[col] = [item.strip() for item in dic_ret[col]]
                    # else:
                    #     if toStr:
                    #         dic_ret[col] = ""
                    #     else:
                    #         dic_ret[col] = []

                except Exception as e:
                    print(e)
                    dic_ret[col] = []
            else:
                dic_ret[col] = dic[col]
        

        return dic_ret

    def etl_col_datetime(self,
                        dic:dict,
                        input_type_is_str:bool=True,
                        str_fmt_in:str=INPUT_DATE_FMT,
                        output_type_is_str:bool=True,
                        str_fmt_out:str=OUTPOUT_DATE_FMT,  
                    )->dict:                                   
        lst_col_datetime=self.LST_COL_DATETIME
        dic_ret = {}
        for col in dic:            
            if col in lst_col_datetime:   
                try:
                    if dic[col] != "":
                        if input_type_is_str:
                            # datetime.strptime -> to datetime object
                            try:
                                dic_ret[col] = datetime.datetime.strptime(dic[col],str_fmt_in)
                            except:
                                dic_ret[col] = dic[col]
                        
                        if output_type_is_str:
                            try:
                                dic_ret[col] = dic_ret[col].strftime(str_fmt_out)
                            except:
                                dic_ret[col] = str(dic_ret[col])

                    else:
                        dic_ret[col] = ""

                except Exception as e:
                    print(e)
                    dic_ret[col] = dic[col]
            else:
                dic_ret[col] = dic[col]

        return dic_ret

    
    def etl_col_int(self,dic:dict)->dict:
        """
        檢查dic中有在傳入的int欄位清單的欄位強制換成int
        如果是空值, 換成字串, 然後回傳dic
        :param dic: 輸入的dictionary
        :param lst_keys: 欄位清單
        :return dic: dictionary
        """
        """
        if dic_data[col] == '<NA>' or dic_data[col] == np.nan:
                    dic_data[col] = '' 
        if col in dic_r_json:
                if dic_r_json[col] != "":
                    try:                
                        dic_r_json[col] = int(dic_r_json[col])            
                    except:
                        pass
        """
        lst_keys=self.LST_COL_INT
        dic_ret = copy.deepcopy(dic)
        for col in lst_keys:
            if col in dic:
                try: #"2018"
                    dic_ret[col] = int(dic[col])                        
                except:
                    try: # np.nan
                        if np.isnan(dic[col]):
                            if col == 'pageView':
                                dic_ret['pageView'] = 1
                            else:                            
                                dic_ret[col] = str(dic_ret[col])
                                if dic_ret[col] == 'nan' or dic_ret[col] == '<NA>':
                                    dic_ret[col] =""
                        else:                                               
                            dic_ret[col] = str(dic_ret[col])
                            if dic_ret[col] == 'nan' or dic_ret[col] == '<NA>':
                                dic_ret[col] =""
                    except: #""
                        dic_ret[col] = str(dic_ret[col])
                        if dic_ret[col] == 'nan' or dic_ret[col] == '<NA>':
                            dic_ret[col] =""
        return dic_ret
    
    def collect_unique_item(self,lst_in:list,lst_unique:list)->list:
        """將list_in 依序取得value,如果不存在於lst_unique中, 就放進去"""
        if len(lst_in) > 0:
            for item in lst_in:
                if item not in lst_unique:
                    lst_unique.append(item)
        else:
            lst_unique=lst_in
        
        lst_unique = list(set(lst_unique))
        
        return lst_unique
        
    def get_lst_dic_by_col(self,
                        dic_query:str={},
                        etlRagicCols:bool=True,
                        use_cache:bool=True,
                        force_update_cache:bool=False,
                        lazy_update_cache:bool=True  
                    )->list:
        """
        查詢指定的eqal的值
        {"colname1":"col1_value",
        "colname2":"col2_value",...}
        """
        lst_dic_ret = []
        str_query=""
        
        if dic_query != {}:
            for colname in dic_query:
                col_name_id = self.dic_tb_col_id[colname]
                str_query += f"&where={col_name_id},eq,{dic_query[colname]}"

        try:
            str_url = self.get_api_url(str_query)
           
            lst_dic_raw = self.get_lst_dic_direct_fetch(str_url,
                                        verifySSL=dic_api["verifySSL"]
                                        )
            if etlRagicCols:
                lst_dic = self.etl_lst_dic_cols(lst_dic_raw)
            
            return lst_dic

        except Exception as e:
            # print(e)
            str_msg = f"get_lst_dic_by_col failed {e}"
            log.error(str_msg)


    def get_lst_dic_by_regex(self,
                            colname:str,
                            lst_items_unique:list=[],
                            etlRagicCols:bool=True,
                            use_cache:bool=True,
                            force_update_cache:bool=False,
                            lazy_update_cache:bool=True  
                        )->list:
        """將colname欄位名稱內符合 lst_items的都撈出來, 回傳list of dict"""
        try:
            str_query=""
            if lst_items_unique != []:
                str_regex_items = "|".join(lst_items_unique)
                col_name_id = self.dic_tb_col_id[colname]
                str_query = f"&where={col_name_id},regex,{str_regex_items}"

            str_url = self.get_api_url(str_query)
            lst_dic_raw = self.get_lst_dic_direct_fetch(str_url,
                                        verifySSL=dic_api["verifySSL"]
                                        )
            if etlRagicCols:
                lst_dic = self.etl_lst_dic_cols(lst_dic_raw)
            
            return lst_dic

        except Exception as e:
            # print(e)
            str_msg = f"get_lst_dic_by_regex failed {e}"
            log.error(str_msg)

    
    def get_df_by_regex(self,
                        colname:str,
                        lst_items_unique:list
                    )->pd.DataFrame:
        """將colname欄位名稱內符合 lst_items的都撈出來, 回傳df"""
        try:
            str_regex_items = "|".join(lst_items_unique)
            col_name_id = self.dic_tb_col_id[colname]
            str_query = f"where={col_name_id},regex,{str_regex_items}"
            str_url = self.get_api_url(str_query)
            df_url_raw =  self.get_df_direct_fetch(str_url)
            df_url = self.etl_df_ragic_cols(df_url_raw)
            return df_url
            
        except Exception as e:
            # print(e)
            str_msg = f"get_df_by_regex failed {e}"
            log.error(str_msg)




if __name__ == "__main__": 

    pass