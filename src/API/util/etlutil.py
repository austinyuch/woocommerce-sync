import copy
from confs import LST_DROP_RAGIC_COLS,MAX_PUBLIC_INTRO_LENGTH
import numpy as np

def get_string_short_intro(str_this:str,
                        int_max_length:int=MAX_PUBLIC_INTRO_LENGTH):
    str_this = str(str_this)
    
    int_length_3 = int_max_length - 3 # 53
    try:
        str_this = (str_this[:int_length_3] + '...')  if len(str_this) > int_max_length else str_this        
        return str_this
    except Exception as e:
        print("get_string_short_intro:"+e)
        return str_this

def get_str_regex(str_query):
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
    
def etl_batch_dic_int_value(dic:dict, lst_keys:list)->dict:
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
    for col in lst_keys:
        if col in dic:
            try: #"2018"
                dic[col] = int(dic[col])                        
            except:
                try: # np.nan
                    if np.isnan(dic[col]):
                        if col == 'pageView':
                            dic['pageView'] = 1
                        else:                            
                            dic[col] = str(dic[col])
                            if dic[col] == 'nan' or dic[col] == '<NA>':
                                dic[col] =""
                    else:                                               
                        dic[col] = str(dic[col])
                        if dic[col] == 'nan' or dic[col] == '<NA>':
                            dic[col] =""
                except: #""
                    dic[col] = str(dic[col])
                    if dic[col] == 'nan' or dic[col] == '<NA>':
                        dic[col] =""
    return dic

          

def etl_drop_dic_ragic_cols(dic_this, 
                        lst_drop_ragic_cols=LST_DROP_RAGIC_COLS
                    ):
    # global LST_DROP_RAGIC_COLS
    # lst_drop_ragic_cols = LST_DROP_RAGIC_COLS
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
    

def etl_dic_cols(dic_this, lst_drop_ragic_cols):
    # global LST_DROP_RAGIC_COLS
    # lst_drop_ragic_cols = LST_DROP_RAGIC_COLS
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


def inv_dict_mapping(dic):
    """
    對調dictionary中每個key-value關係; 用於欄位名稱轉換
    """
    return {v: k for k, v in dic.items()}


def dict_col_rename_mapping(dic_data, dic_map):
    """
    對dictionary做欄位(key)名稱轉換
    :param dic_data: 傳入的資料dictionary
    :param dic_map: key名稱對照表
    :return: 轉換後key名稱之dictionary
    """
    dic_params = {}
    for key in dic_data:
        try:
            str_col_id = dic_map[key]
            dic_params[str_col_id] = dic_data[key]
        except:
            pass
    return dic_params


def etl_col_multi(dic:dict,sep:str="|",lst_col_multi:list=[])->dict:
    # lst_col_multi=self.LST_COL_MULTI
    dic_ret = {}
    """將所有|分隔的欄位的字串都變成list"""
    for col in dic:            
        if col in lst_col_multi:            
            try:
                if dic[col] != "":
                    dic_ret[col] = dic[col]
                    dic_ret[col] = [item.strip() for item in dic_ret[col].split(sep)]
                else:
                    dic_ret[col] = []
            except Exception as e:
                print(e)
                dic_ret[col] = []
        else:
            dic_ret[col] = dic[col]

    return dic_ret