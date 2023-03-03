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
from enum import Enum
import sqlite3
import requests
import matplotlib.pyplot as plt
import seaborn as sns


# XBRLをpython形式に変換するライブラリのフォルダパス
##sys.path.append(r'D:\\教育\\財務３表\\EdinetData')
import urllib3
from urllib3.exceptions import InsecureRequestWarning
urllib3.disable_warnings(InsecureRequestWarning)

class analysysEdinetCodeSummaryStatistics:
    def __init__(self):
         #dbパス設定
        self._dbPath = "F:/教育/Edinet/Template/EdinetData.db"
        print(self._dbPath)

        self.get要約統計量()

    # Select文実行
    def executeSelectQuery(self, query : str)-> pd.DataFrame:
        conn = sqlite3.connect(self._dbPath)  
        df = pd.read_sql_query(sql=query, con=conn)
        df.reset_index()
        #close
        conn.close()
        return df
    
    # Insert文実行
    def executeInsertQuery(self, query : str):
        conn = sqlite3.connect(self._dbPath)  
        cur = conn.cursor()
        cur.execute(query)
        #close
        conn.commit()
        conn.close()    

    ''''''
    def get要約統計量(self):
        df = self.executeSelectQuery("Select [提出者業種], [Year] FROM View提出者有価証券リスト Group By [提出者業種], [Year]")
        for index, row in df.iterrows():
            year = row['Year']
            業種 = row['提出者業種']

            項目s = {"売上高","純利益", "資産","売上高利益率","資本回転率"}
            dfData = self.executeSelectQuery("Select [売上高],[純利益],[資産],[売上高利益率],[資本回転率] FROM [ViewAna有価証券リスト] where [Year] = " + str(year) + " AND [提出者業種] like '" + 業種 + "'")

            for 項目 in 項目s:
                min = dfData[項目].quantile(0)
                one_fourth = dfData[項目].quantile(0.25)
                median = dfData[項目].median()
                three_fourths = dfData[項目].quantile(0.75)
                max = dfData[項目].quantile(1.0)

                sqlTemplateDetail = "INSERT into [有価証券リスト要約統計量_{項目}] ([Year],[業種],[min],[one_fourth],[median],[three_fourths],[max]) VALUES ({Year},'{業種}',{min},{one_fourth},{median},{three_fourths},{max})"
                sqlTemplateDetail = sqlTemplateDetail.replace("{項目}", 項目)
                sqlTemplateDetail = sqlTemplateDetail.replace("{Year}", str(year))
                sqlTemplateDetail = sqlTemplateDetail.replace("{業種}", 業種)
                sqlTemplateDetail = sqlTemplateDetail.replace("{min}", str(min))
                sqlTemplateDetail = sqlTemplateDetail.replace("{one_fourth}", str(one_fourth))
                sqlTemplateDetail = sqlTemplateDetail.replace("{median}", str(median))
                sqlTemplateDetail = sqlTemplateDetail.replace("{three_fourths}", str(three_fourths))
                sqlTemplateDetail = sqlTemplateDetail.replace("{max}", str(max))
                self.executeInsertQuery(sqlTemplateDetail)

analysysEdinetCodeSummaryStatistics()