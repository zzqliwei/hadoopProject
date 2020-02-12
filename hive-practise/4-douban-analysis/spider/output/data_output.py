# -*-coding:utf-8-*-


class Output(object):
    def __init__(self):
        pass

    def output_video_links(self, video_links, video_type):
        with open("file_output/links/{0}-video_links.csv".format(video_type), 'w') as fp_links:
            for link in video_links:
                fp_links.write(link+"\n")
        print("one tag movies links write OK !")

    def output_video_detail(self, video_message, video_id, video_type):
        with open("file_output/{0}-video.csv".format(video_type), "a", encoding="UTF-8") as fp_one:
            fp_one.write(video_id + "\t" + video_message["name"] + "\t" +
                         video_message['year'] + "\t" +
                         video_message['director'] + "\t" +
                         video_message["scripts_writer"] + "\t" +
                         video_message["star"] + "\t" +
                         video_message["category"] + "\t" +
                         video_message["nation"] + "\t" +
                         video_message["language"] + "\t" +
                         video_message["showtime"] + "\t" +
                         video_message["initial_release_date"] + "\t" +
                         video_message["comment_num"] + "\t" +
                         video_message["comment_score"] + "\t" +
                         video_message["summary"] + "\n")
        print("One Movie Write OK !\n")