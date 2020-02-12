# -*-coding:utf-8-*-

from selenium import webdriver
import time

chrome_driver = "E:\\bigdata-course\\SQL-on-hadoop\\Hive\\chromedriver.exe"


class HtmlFetcher(object):
    def __init__(self):
        pass


    def fetch_video_links(self, url, num):
        browser = webdriver.Chrome(chrome_driver)
        browser.get(url)
        time.sleep(3)
        count = 0
        while True:
            time.sleep(2)
            try:
                load_more_tag = browser.find_element_by_link_text("加载更多")
                if load_more_tag is None:
                    break
                load_more_tag.click()
                count += 1
                if num is not None and count == num:
                    break
            except Exception as e:
                print(e)
                break
        time.sleep(3)
        return browser.page_source.__str__()

    def fetch_video_detail(self, url):
        browser = webdriver.Chrome(chrome_driver)
        browser.get(url)
        try:
            load_more_tag = browser.find_element_by_class_name("more-actor")
            load_more_tag.click()
        except Exception as e:
            print(e)
            pass
        return browser.page_source.__str__()