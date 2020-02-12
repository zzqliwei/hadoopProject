# -*-coding:utf-8-*-
from bs4 import BeautifulSoup
import re


class HtmlParser(object):
    def __init__(self):
        pass

    def parse_video_links(self, page_html_content):
        video_links = []
        page = BeautifulSoup(page_html_content)
        items = page.find_all(class_="item")  # 定位到每个电影的div
        print(len(items))
        try:
            for item in items:   # 解析得到每部电影的详情链接
                href = item.attrs["href"]
                print("href:\t", href)
                video_links.append(href)
        except Exception as e:
            print(e)
            pass
        return video_links     # 返回该页的电影详情链接列表

    def parse_video_detail(self, page_html_content, video_type):
        one_video_message = {}   # 定义一个字典用来存放一部电影的相关信息
        page = BeautifulSoup(page_html_content)
        # 需要获取的属性包括 电影ID/电影名字/导演/编剧/类型/主演/上映时间/片长/剧情简介/评价人数/豆瓣评分
        try:
            name = page.find(property="v:itemreviewed").get_text()     # 电影名字
        except:
            name = "-"

        try:
            year = page.find(class_="year").get_text()[1:5]     # 电影年份
        except:
            year = "-"

        nation_desc_text = page.find(id="info").get_text().replace("\n", " ")
        if video_type == "movie":
            if "官方网站:" in nation_desc_text:
                pattern = '.*导演:(.*)编剧:(.*)主演:(.*)' \
                          '类型:(.*)官方网站:.*制片国家/地区:(.*)语言:(.*)上映日期:(.*)' \
                          '片长: (\d+).*分钟.*'
            else:
                pattern = '.*导演:(.*).*编剧:(.*).*主演:(.*).*' \
                          '类型:(.*).*制片国家/地区:(.*).*语言:(.*).*上映日期:(.*).*' \
                          '片长: (\d+).*分钟.*'

            result = re.match(pattern, nation_desc_text)
            director = result.group(1)
            scripts_writer = result.group(2)
            star = result.group(3)
            category = result.group(4)
            nation = result.group(5)
            language = result.group(6)
            initial_release_date = result.group(7)
            showtime = result.group(8)

        try:
            comment_num = page.find(property="v:votes").get_text()      #评价人数
        except:
            comment_num = "0"
        try:
            comment_score = page.find(property="v:average").get_text()  #豆瓣评分
        except:
            comment_score = "0.0"

        try:
            summary = page.find(property="v:summary").get_text()        # 剧情简介
        except:
            summary = ""

        one_video_message['name'] = name.strip().replace(",", "-")
        one_video_message['year'] = year.strip()
        one_video_message['director'] = director.strip()
        one_video_message["scripts_writer"] = scripts_writer.strip()
        one_video_message["star"] = star.strip()
        one_video_message["category"] = category.strip()
        one_video_message["nation"] = nation.strip()
        one_video_message["language"] = language.strip()
        one_video_message["showtime"] = showtime.strip()
        one_video_message["initial_release_date"] = initial_release_date.strip()
        one_video_message["comment_num"] = comment_num.strip()
        one_video_message["comment_score"] = comment_score.strip()
        one_video_message["summary"] = summary.strip().replace(" ", "").replace("\n","")
        return one_video_message
