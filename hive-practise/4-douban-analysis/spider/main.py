# -*-coding:utf-8-*-
import download.down_html
import parse.parse_html
import output.data_output


class DoubanSprider(object):
    def __init__(self):
        self.fetcher = download.down_html.HtmlFetcher()
        self.parser = parse.parse_html.HtmlParser()
        self.outputer = output.data_output.Output()

    def fetch_one_tag_all_video_links(self, url, video_type):
        # 1、下载对应tag的视频的列表的html页面
        page_html_content = self.fetcher.fetch_video_links(url, 2)
        # 2、解析下载下来的html页面，主要是将视频对应的链接解析出来
        video_links = self.parser.parse_video_links(page_html_content)
        # 3、将解析出来的链接保存到本地文件
        self.outputer.output_video_links(video_links, video_type)
        return video_links

    def fetch_one_tag_all_video_detail(self, video_links, video_type):
        # 1、循环video_links， 处理每一个视频的详细信息的链接
        for link in video_links:
            page_html_content = self.fetcher.fetch_video_detail(link)  # 2、下载对应的视频的详细信息的链接的html网页
            # 3、解析视频的详细信息的网页，得到视频的详细信息
            one_video_message = self.parser.parse_video_detail(page_html_content, video_type)
            # 4、将每一个视频的详细信息保存到本地文件
            video_id = link.split("/")[-2]
            self.outputer.output_video_detail(one_video_message, video_id, video_type)


if __name__ == "__main__":
    print("Starting fetch data from douban")
    spider = DoubanSprider()
    video_links = spider.fetch_one_tag_all_video_links("https://movie.douban.com/tag/#/?tags=电影", "movie")
    spider.fetch_one_tag_all_video_detail(video_links, "movie")
