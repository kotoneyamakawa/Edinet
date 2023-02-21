import datetime as dt
from datetime import timedelta
import os
import sys
import requests
import json
from zipfile import ZipFile
import pandas as pd
import glob
from zipfile import ZipFile
from collections import OrderedDict
from os.path import basename as os_basename
from bs4 import BeautifulSoup
import pyodbc
from enum import Enum
import sqlite3
import urllib3
from urllib3.exceptions import InsecureRequestWarning
urllib3.disable_warnings(InsecureRequestWarning)

class loadEdinet():
    def __init__(self):
        #dbパス設定
        self._dbPath = os.path.join(os.getcwd(), '../Template/EdinetData.db')
        print(self._dbPath) 

        # 会社のリストを読み込み
        self._dfEdinetCode_会社名 = self.getEdinetData()

        # 取得期間の設定：直近n日分
        self._end = dt.datetime.today()
        self._start = dt.datetime(2021,12,24) #dt.datetime.today() - timedelta(days=365*5)     

        #指定したディレクトリにデータをコピー
        self._rootPath = os.path.join(os.getcwd(), "../EdinetData")
        self.downloadData()

    # Select文実行
    def executeSelectQuery(self, query : str)-> pd.DataFrame:
        conn = sqlite3.connect(self._dbPath)  
        df = pd.read_sql_query(sql=query, con=conn)
        #close
        conn.close()
        return df

    # Select文実行
    def executeInsertQuery(self, query : str):
        conn = sqlite3.connect(self._dbPath)  
        cur = conn.cursor()
        cur.execute(query)
        #close
        conn.commit()
        conn.close()

    '''DBからEDINETデータ（コード、会社名）取得'''
    def getEdinetData(self)->dict:
        dictEdinetCode_会社名 = {}

        df = self.executeSelectQuery("SELECT [EDINETCODE],[提出者名] FROM EdinetCode Where [提出者業種] like '建設業' or [提出者業種] like '不動産業' or [提出者業種] like '情報・通信業' ")
        return df

    '''日数リストを作成'''
    def make_day_list(self)-> list:
        print("start_date：", self._start)
        print("end_day：", self._end)

        period = self._end - self._start
        period = int(period.days)
        day_list = []
        for d in range(period):
            day = self._start + timedelta(days=d)
            day_list.append(day)
        day_list.append(self._end)
        return day_list

    def downloadData(self):
        '''指定した期間、報告書種類、会社で報告書を取得し、取得したファイルのパスの辞書を返す'''
        #取得期間の日付リストを作成
        day_term = self.make_day_list()

        print('EDINETへのアクセスを開始')
        for i,day in enumerate(day_term):
            url = "https://disclosure.edinet-fsa.go.jp/api/v1/documents.json"
            params = {"date": day.strftime('%Y-%m-%d'), "type": 2}
            
            # 進捗表示
            if i % 50 == 0:
                print(f'{i}日目：{day}を開始')
            
            # EDINETから1日の書類一覧を取得
            res = requests.get(url, params=params, verify=False)
            
            # 必要な書類の項目を抜き出し
            if res.ok:
                json_data = res.json()

                if json_data['metadata']['status'] == "400":
                    continue
                
                for num in range(len(json_data["results"])):
                    # 指定した会社の指定した書類を抜き出し
                    data = json_data["results"][num]

                    # 取り下げフラグ=0以外は対象外
                    if data['withdrawalStatus'] != '0':
                        continue
                    # XBRLがなければ対象外
                    if data['xbrlFlag'] != '1':
                        continue

                    df = self._dfEdinetCode_会社名[self._dfEdinetCode_会社名['EDINETCODE'] == data['edinetCode']]
                    if not( data['ordinanceCode'] == "010" and len(df) > 0):
                        continue

                    # 有価証券報告書に絞る
                    form_code = data["formCode"]
                    if form_code !="030000" and form_code !="030001":
                        continue

                    #保存先ディレクトリー＞会社名_EDINETCode\yyyy\yyyymmdd(s)-yyyyMMdd(E)
                    tdata = ""
                    if(form_code =="030000" or form_code =="043000"):
                        tdate = dt.datetime.strptime(data['periodStart'],'%Y-%m-%d')
                    else:
                        tdate = dt.datetime.strptime(data['submitDateTime'],'%Y-%m-%d %H:%M')
                    directory = "\\" + data['filerName'] + '_' + data['edinetCode'] + "\\" + str(tdate.year) + "\\" + data['docDescription']
                    
                    # まだ持っていないファイルをダウンロード
                    file_path =  os.path.join(self._rootPath + directory, "data.zip")
                    if os.path.isfile(file_path):
                        continue

                    # 有価証券リストへInsert
                    sqlTemplate = "INSERT into [有価証券リスト] ([DocID],[EDINETCODE],[Year],[報告日],[Path]) VALUES ('{docID}','{Code}',{Year},'{報告日}','{Path}')"
                    sqlTemplate = sqlTemplate.replace("{docID}",data['docID'])
                    sqlTemplate = sqlTemplate.replace("{Code}",data['edinetCode'])
                    sqlTemplate = sqlTemplate.replace("{Path}",file_path)

                    tdata = ""
                    form_code = data["formCode"]
                    if(form_code =="030000" or form_code =="043000"):
                        tdate = dt.datetime.strptime(data['periodStart'],'%Y-%m-%d')
                    else:
                        tdate = dt.datetime.strptime(data['submitDateTime'],'%Y-%m-%d %H:%M')    
                    sqlTemplate = sqlTemplate.replace("{Year}", str(tdate.year))
                    sqlTemplate = sqlTemplate.replace("{報告日}", tdate.strftime('%Y%m%d%H%M%S'))
                    self.executeInsertQuery(sqlTemplate)

                    # ファイルを取得
                    url_zip = "https://disclosure.edinet-fsa.go.jp/api/v1/documents/" + data['docID']
                    params_zip = {"type": 1}

                    # データのDL
                    res_zip = requests.get(url_zip, params=params_zip, verify=False, stream=True)
                    if not os.path.isdir(self._rootPath + directory):
                        os.makedirs(self._rootPath + directory)
                    # zipとして保存
                    if res_zip.status_code == 200:
                        with open(file_path, 'wb') as f:
                            for chunk in res_zip.iter_content(chunk_size=1024):
                                if chunk:
                                    f.write(chunk)
                                    f.flush()

test = loadEdinet()
