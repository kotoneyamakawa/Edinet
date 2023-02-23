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

class extractionEdinetDataResearchAndDevelopment:

    def __init__(self):
         #dbパス設定
        self._dbPath = "D:/教育/Edinet/Template/EdinetData.db"
        print(self._dbPath) 
        self.extract研究開発費()

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

    #研究開発費抽出
    def extract研究開発費(self):
        df有価証券 = self.executeSelectQuery("Select * FROM [View提出者有価証券リスト]")

        for index, row有価証券 in df有価証券.iterrows():
            df研究開発費 = self.executeSelectQuery( "Select * From 研究開発費 Where DocID like '"+ row有価証券["DocID"] + "'")
            if len(df研究開発費) > 0:
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

                sqlTemplateDetail = "INSERT into [研究開発費] ([EDINETCODE],[Year],[DocID],[出力項目]) VALUES ('{EDINETCODE}',{Year},'{DocID}','{出力項目}')"
                sqlTemplateDetail = sqlTemplateDetail.replace("{DocID}", row有価証券['DocID'])
                sqlTemplateDetail = sqlTemplateDetail.replace("{EDINETCODE}", row有価証券["EDINETCODE"])
                sqlTemplateDetail = sqlTemplateDetail.replace("{Year}", str(row有価証券["Year"]))

                # zipアーカイブから対象ファイルを読み込む
                textValue = ""
                for info in infos:
                    filename = os_basename(info.filename)

                    # zipからhtmデータを読み込んで辞書に入れる
                    if not filename.endswith('.htm'):
                        continue

                    # htmファイル読み込み
                    zip_obj.extract(info.filename)
                    ff = open( info.filename , "r" ,encoding="utf-8" ).read() 
                    soup = BeautifulSoup( ff ,"html.parser")
                    results = soup.find_all("ix:nonnumeric", attrs={'name': "jpcrp_cor:ResearchAndDevelopmentActivitiesTextBlock"})     
                    for result in results:
                        if not result.text:
                            continue

                        for text in result.strings:
                           textValue += text

                textValue = textValue.replace("'", '"')
                sqlTemplateDetail = sqlTemplateDetail.replace("{出力項目}",textValue)
                self.executeInsertQuery(sqlTemplateDetail)


extractionEdinetDataResearchAndDevelopment()
