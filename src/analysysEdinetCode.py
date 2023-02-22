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
import matplotlib.pyplot as plt
import japanize_matplotlib
import seaborn as sns


# XBRLをpython形式に変換するライブラリのフォルダパス
##sys.path.append(r'D:\\教育\\財務３表\\EdinetData')
import urllib3
from urllib3.exceptions import InsecureRequestWarning
urllib3.disable_warnings(InsecureRequestWarning)

class analysysEdinetCode:
    def __init__(self):
         #dbパス設定
        self._dbPath = "D:/教育/Edinet/Template/EdinetData.db"
        print(self._dbPath)

        self.plot全体()

    # Select文実行
    def executeSelectQuery(self, query : str)-> pd.DataFrame:
        conn = sqlite3.connect(self._dbPath)  
        df = pd.read_sql_query(sql=query, con=conn)
        df.reset_index()
        #close
        conn.close()
        return df

    ''''''
    def plot全体(self):
        df = self.executeSelectQuery("Select [提出者業種], [Year] FROM View提出者有価証券リスト Group By [提出者業種], [Year]")
        for index, row in df.iterrows():
            year = row['Year']
            業種 = row['提出者業種']
            dfAna = self.executeSelectQuery("Select * FROM [ViewAna有価証券リスト] where [Year] = " + str(year) + " AND [提出者業種] like '" + 業種 + "'")
            plot = sns.pairplot(dfAna) 
            plot.savefig("D:/教育/Edinet/ana/images/" + 業種 + "ALL_" + str(year) + ".png")
            plt.clf()
            plt.close()

analysysEdinetCode()