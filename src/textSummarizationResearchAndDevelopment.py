import requests
import json
import sqlite3
import pandas as pd
import shutil

class textSummarizationResearchAndDevelopment:

    def __init__(self):
         #dbパス設定
        self._dbPath = "D:/教育/Edinet/Template/EdinetData.db"
        self._htmlTemp = "D:/教育/Edinet/Template/TemplateRes.html"
        self._outputDir = "D:/教育/Edinet/ana/企業Res/"        
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

    def extract研究開発費(self):  
        df = self.executeSelectQuery("Select Distinct [EDINETCODE], [提出者名],[提出者業種] FROM [ViewAna有価証券リスト] group by [EDINETCODE], [提出者名],[提出者業種]")
        for index, row in df.iterrows():
            EdinetCode = row['EDINETCODE']
            提出者名 = row['提出者名']

            #htmlをコピー
            file_name = self._outputDir + 提出者名 + ".html"
            shutil.copyfile(self._htmlTemp, file_name)

            with open(file_name, encoding="UTF-8") as f:
                data_lines = f.read()          

            df研究開発費 = self.executeSelectQuery( "Select [Year],[出力項目] From 研究開発費 Where EDINETCODE like '" + EdinetCode +"'")
            if len(df研究開発費) == 0:
                continue
            
            strValue研究開発費 = ""
            for index, row研究開発費 in df研究開発費.iterrows():

                # エンドポイント
                url = 'https://api.a3rt.recruit.co.jp/text_summarization/v1'

                # APIキー
                key = 'DZZYLRJ2Es9F9au7Qc6FR4QgdiVWbmsG'

                #要約する文章
                sentence = row研究開発費['出力項目']

                sentences = sentence.splitlines()

                strValue = ""
                for item in sentences:
                    if item == '\n' or item == '':
                        continue

                    #パラメーターの設定
                    params = {
                        'sentences': item,
                        'apikey': key,
                    'linenumber': '1' #抽出文章数
                    }

                    #リクエスト
                    res = requests.post(url, data=params)
                    values = json.loads(res.text)
                    if values["status"] != 0:
                        strValue += item + '\n'
                        continue

                    #レスポンスから要約されたテキストを取り出す
                    for summary in values["summary"]:
                        strValue += summary + '\n'

                strValue研究開発費 += "<h2>{Year}</h2><div>{出力項目}</div>"
                strValue研究開発費 = strValue研究開発費.replace("{Year}", str(row研究開発費['Year']))
                strValue研究開発費 = strValue研究開発費.replace("{出力項目}", strValue)                  

            # 文字列置換
            data_lines = data_lines.replace("//	研究開発費_data", strValue研究開発費)

            # 同じファイル名で保存
            with open(file_name, mode="w", encoding="UTF-8") as f:
                f.write(data_lines)            

textSummarizationResearchAndDevelopment()