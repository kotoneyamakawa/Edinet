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
import shutil


# XBRLをpython形式に変換するライブラリのフォルダパス
##sys.path.append(r'D:\\教育\\財務３表\\EdinetData')
import urllib3
from urllib3.exceptions import InsecureRequestWarning
urllib3.disable_warnings(InsecureRequestWarning)

class outputCompanyData:
    def __init__(self):
         #dbパス設定
        self._dbPath = "F:/教育/Edinet/Template/EdinetData.db"
        self._htmlTemp = "F:/教育/Edinet/Template/Template.html"
        self._outputDir = "F:/教育/Edinet/ana/企業/"
        print(self._dbPath)

        self.outputCompany()

    # Select文実行
    def executeSelectQuery(self, query : str)-> pd.DataFrame:
        conn = sqlite3.connect(self._dbPath)  
        df = pd.read_sql_query(sql=query, con=conn)
        df.reset_index()
        #close
        conn.close()
        return df
 

    ''''''
    def outputCompany(self):
        df = self.executeSelectQuery("Select Distinct [EDINETCODE], [提出者名],[提出者業種] FROM [ViewAna有価証券リスト] group by [EDINETCODE], [提出者名],[提出者業種]")
        for index, row in df.iterrows():
            EdinetCode = row['EDINETCODE']
            提出者名 = row['提出者名']
            業種 = row['提出者業種']

            #htmlをコピー
            file_name = self._outputDir + 提出者名 + ".html"
            shutil.copyfile(self._htmlTemp, file_name)

            with open(file_name, encoding="UTF-8") as f:
                data_lines = f.read()

            項目s = {"売上高","純利益", "資産","売上高利益率","資本回転率"}
            for 項目 in 項目s:
                dfData = self.executeSelectQuery("Select [Year]," + 項目 +", [one_fourth], [median], [three_fourths] FROM [ViewLineGraph" + 項目 + "] where [EdinetCode] like '" + EdinetCode + "'")
                strValue = ""
                for row in dfData.itertuples():
                    strValue += "[{1},{2},{3},{4},{5}],"
                    strValue = strValue.replace("{1}", str(row[1]))
                    strValue = strValue.replace("{2}", str(row[2]))
                    strValue = strValue.replace("{3}", str(row[3]))
                    strValue = strValue.replace("{4}", str(row[4]))
                    strValue = strValue.replace("{5}", str(row[5]))

                
                # 文字列置換
                data_lines = data_lines.replace("//	"+ 項目 +"_data", strValue)

            #研究開発費
            strValue研究開発費 = ""
            dfData研究開発費 = self.executeSelectQuery("Select [Year],[出力項目] FROM [ViewAna研究開発費] where [EdinetCode] like '" + EdinetCode + "'")
            for row研究開発費 in dfData研究開発費.itertuples():
                strValue研究開発費 += "<h2>{Year}</h2><div>{出力項目}</div>"
                strValue研究開発費 = strValue研究開発費.replace("{Year}", str(row研究開発費[1]))
                strValue研究開発費 = strValue研究開発費.replace("{出力項目}", row研究開発費[2])      

            # 文字列置換
            data_lines = data_lines.replace("//	研究開発費_data", strValue研究開発費)

            # 同じファイル名で保存
            with open(file_name, mode="w", encoding="UTF-8") as f:
                f.write(data_lines)

outputCompanyData()