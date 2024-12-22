import requests
from bs4 import BeautifulSoup
from retry import retry
import urllib
import time
import numpy as np
import urllib.parse
import slackweb
import pandas as pd

# slack通知用
webhook = "https://hooks.slack.com/services/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"

# データサンプルの初期化
data_samples = []

# スクレイピングするページ数
max_page = 20

# SUUMO民泊可能物件のURL
url = "https://suumo.jp/jj/chintai/ichiran/FR301FC001/?ar=030&bs=040&pc=30&smk=&po1=25&po2=99&shkr1=03&shkr2=03&shkr3=03&shkr4=03&sc=13101&sc=13102&sc=13103&sc=13104&sc=13105&sc=13113&sc=13106&sc=13107&sc=13108&sc=13118&sc=13121&sc=13122&sc=13123&sc=13109&sc=13110&sc=13111&sc=13112&sc=13114&sc=13115&sc=13120&sc=13116&sc=13117&sc=13119&ta=13&cb=0.0&ct=10.0&et=9999999&mb=0&mt=9999999&cn=9999999&fw2=%E6%B0%91%E6%B3%8A%E5%8F%AF"

# リトライ付きのリクエスト関数
@retry(tries=3, delay=10, backoff=2)
def load_page(url):
    html = requests.get(url)
    soup = BeautifulSoup(html.content, 'html.parser')
    return soup

# カラム名の定義
columns = [
    "カテゴリ", "建物名", "住所", "最寄り駅1", "最寄り駅2", "最寄り駅3",
    "築年数", "階数", "階", "家賃", "管理費", "敷金", "礼金",
    "間取り", "面積", "URL"
]

# データ収集処理
for page in range(1, max_page + 1):
    soup = load_page(url.format(page))
    mother = soup.find_all(class_='cassetteitem')

    for child in mother:
        data_home = [
            child.find(class_='ui-pct ui-pct--util1').text.strip(),
            child.find(class_='cassetteitem_content-title').text.strip(),
            child.find(class_='cassetteitem_detail-col1').text.strip(),
        ]

        # 最寄り駅
        children = child.find(class_='cassetteitem_detail-col2')
        for grandchild in children.find_all(class_='cassetteitem_detail-text'):
            data_home.append(grandchild.text.strip())

        # 築年数と階数
        children = child.find(class_='cassetteitem_detail-col3')
        for grandchild in children.find_all('div'):
            data_home.append(grandchild.text.strip())

        # 部屋情報
        rooms = child.find(class_='cassetteitem_other')
        for room in rooms.find_all(class_='js-cassette_link'):
            data_room = []

            for id_, grandchild in enumerate(room.find_all('td')):
                if id_ == 2:
                    data_room.append(grandchild.text.strip())
                elif id_ == 3:
                    data_room.append(grandchild.find(class_='cassetteitem_other-emphasis ui-text--bold').text.strip())
                    data_room.append(grandchild.find(class_='cassetteitem_price cassetteitem_price--administration').text.strip())
                elif id_ == 4:
                    data_room.append(grandchild.find(class_='cassetteitem_price cassetteitem_price--deposit').text.strip())
                    data_room.append(grandchild.find(class_='cassetteitem_price cassetteitem_price--gratuity').text.strip())
                elif id_ == 5:
                    data_room.append(grandchild.find(class_='cassetteitem_madori').text.strip())
                    data_room.append(grandchild.find(class_='cassetteitem_menseki').text.strip())
                elif id_ == 8:
                    get_url = grandchild.find(class_='js-cassette_link_href cassetteitem_other-linktext').get('href')
                    abs_url = urllib.parse.urljoin(url, get_url)
                    data_room.append(abs_url)

            data_samples.append(data_home + data_room)

# データフレーム化と保存
df = pd.DataFrame(data_samples, columns=columns)
df = df.drop_duplicates()

# 指定したカラムをまとめてslack通知入れる
num = 1
for _, row in df.iterrows():
    slack_txt = str(f"[SUUMO定期実行-{num}] 建物名: {row['建物名']}, 家賃: {row['家賃']}, 住所: {row['住所']}, 最寄り駅1: {row['最寄り駅1']}, 築年数	: {row['築年数']}, 間取り: {row['間取り']},面積: {row['面積']},URL: {row['URL']}")
    # print(txt)

    slack = slackweb.Slack(url=webhook)
    slack.notify(text=slack_txt)

# df.to_csv("suumo_data.csv", index=False, encoding="utf-8-sig")
# print(df.head())
