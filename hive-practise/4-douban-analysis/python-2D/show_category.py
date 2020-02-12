#encoding:utf-8

# 需要在cmd中执行： pip install matplotlib
from matplotlib import style
import matplotlib.pyplot as plt
plt.rcParams['font.sans-serif'] = ['SimHei']  # 用来正常显示中文标签
plt.rcParams['axes.unicode_minus'] = False  # 用来正常显示负号

style.use("ggplot")
# 加载数据
category_number = {}
with open("000000_0", "r", encoding="UTF-8") as fp:
    for line in fp.readlines():
        category_number[line.strip().split("\t")[0]] = int(line.strip().split("\t")[1])
category = []
number = []
category_number = sorted(category_number.items(), key=lambda dic: dic[1], reverse=True)  # 字典排序
for name, num in category_number[:10]:   # 得到对应类别电影数目最多的10个类别
    category.append(name)
    number.append(num)

fig, ax = plt.subplots()
# plt.plot(range(1,len(category)+1),number) #折线图
rects1 = plt.bar(range(1, len(category)+1), number, width=0.4, alpha=0.2, color='g',align="center")  # 条形图


def autolabel(rects):
    for rect in rects:
        height = rect.get_height()
        ax.text(rect.get_x()+rect.get_width()/2.0, 1.05*height, '%d' % int(height), ha='center', va='bottom')


autolabel(rects1)

plt.xlabel(u"category", color='r')  # 横坐标
plt.ylabel(u"movie amount", color='r')     # 纵坐标
plt.title(u'category-amount 分布图')           # 图片名字
plt.xticks(range(1, len(category)+1), category, fontsize=12)      # x轴加上类别名称
plt.yticks(fontsize=10)            # y坐标数字显示大小
plt.savefig(u'top10category.png')
plt.show()     # 显示