import datetime as dt
from datetime import timedelta
import os
from zipfile import ZipFile
import pandas as pd
import pathlib
from zipfile import ZipFile
from collections import OrderedDict
from os.path import basename as os_basename
from bs4 import BeautifulSoup
import pyodbc
from enum import Enum
import sqlite3
import requests

# XBRLをpython形式に変換するライブラリのフォルダパス
##sys.path.append(r'D:\\教育\\財務３表\\EdinetData')
import urllib3
from urllib3.exceptions import InsecureRequestWarning
urllib3.disable_warnings(InsecureRequestWarning)

class extractionEdinetData:

    def __init__(self):
         #dbパス設定
        self._dbPath = "D:/教育/Edinet/Template/EdinetData.db"
        print(self._dbPath) 

        self.extractBSPL()

    # Select文実行
    def executeSelectQuery(self, query : str)-> pd.DataFrame:
        conn = sqlite3.connect(self._dbPath)  
        df = pd.read_sql_query(sql=query, con=conn)
        df.reset_index()
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

    #BS,PLの値抽出
    def extractBSPL(self):
        df有価証券 = self.executeSelectQuery("Select * FROM [View提出者有価証券リスト]")
        df分析項目 = self.executeSelectQuery("Select [プレフィックス] || ':' || [要素名] as name, [大項目],[項目名] From [分析項目Detail]")
        name_list = df分析項目['name'].tolist()

        for index, row有価証券 in df有価証券.iterrows():
            dfDetail = self.executeSelectQuery("Select * From [有価証券リストALL] where DocID like '" + row有価証券["DocID"] + "'")
            if len(dfDetail) > 0:
                continue
            
            path = row有価証券["Path"]
            p = pathlib.Path(path)
            if not os.path.isfile(p):
                    # ファイルを取得
                    url_zip = "https://disclosure.edinet-fsa.go.jp/api/v1/documents/" + row有価証券['DocID']
                    params_zip = {"type": 1}

                    # データのDL
                    res_zip = requests.get(url_zip, params=params_zip, verify=False, stream=True)
                    # zipとして保存
                    if res_zip.status_code == 200:
                        with open(p, 'wb') as f:
                            for chunk in res_zip.iter_content(chunk_size=1024):
                                if chunk:
                                    f.write(chunk)
                                    f.flush()                

            with ZipFile(p, 'r') as zip_obj:
                # ファイルリスト取得
                infos = zip_obj.infolist()

                sqlTemplateDetail = "INSERT into [有価証券リストALL] ([EDINETCODE],[Year],[DocID],[提出者名],[提出者業種],[分析項目大分類],[分析項目項目],[Value]) VALUES ('{EDINETCODE}',{Year},'{DocID}','{提出者名}','{提出者業種}','{分析項目大分類}','{分析項目項目}',{Value})"
                sqlTemplateDetail = sqlTemplateDetail.replace("{DocID}", row有価証券['DocID'])
                sqlTemplateDetail = sqlTemplateDetail.replace("{EDINETCODE}", row有価証券["EDINETCODE"])
                sqlTemplateDetail = sqlTemplateDetail.replace("{Year}", str(row有価証券["Year"]))
                sqlTemplateDetail = sqlTemplateDetail.replace("{提出者名}", row有価証券["提出者名"])
                sqlTemplateDetail = sqlTemplateDetail.replace("{提出者業種}", row有価証券["提出者業種"])

                # zipアーカイブから対象ファイルを読み込む
                bfind = False
                for info in infos:
                    if bfind:
                        break
                    filename = os_basename(info.filename)

                    # zipからhtmデータを読み込んで辞書に入れる
                    if not filename.endswith('.htm'):
                        continue

                    # htmファイル読み込み
                    zip_obj.extract(info.filename)
                    ff = open( info.filename , "r" ,encoding="utf-8" ).read() 
                    soup = BeautifulSoup( ff ,"html.parser")
                    results = soup.find_all("ix:nonfraction", attrs={'name': name_list, 'contextref':{'CurrentYearInstant','CurrentYearDuration', 'CurrentYearDuration_NonConsolidatedMember','CurrentYearInstant_NonConsolidatedMember'}})     
                    for result in results:
                        if not result.text:
                            continue

                        df = df分析項目[df分析項目['name']==result.get("name")]
                        for index, row分析項目 in df.iterrows():
                            sqlTemplateDetailTmp = sqlTemplateDetail
                            sqlTemplateDetailTmp = sqlTemplateDetailTmp.replace("{分析項目大分類}",row分析項目['大項目'])
                            sqlTemplateDetailTmp = sqlTemplateDetailTmp.replace("{分析項目項目}",row分析項目['項目名'])
                            nValue = int(result.text.replace(",",""))
                            nScale = int(result.get('scale'))
                            sqlTemplateDetailTmp = sqlTemplateDetailTmp.replace("{Value}",str(nValue * pow(10, nScale)))
                            self.executeInsertQuery(sqlTemplateDetailTmp)
                            break


extractionEdinetData()
