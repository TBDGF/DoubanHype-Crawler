import asyncio
import datetime
import re
import traceback
from asyncio import sleep
import aiohttp
import pymysql
from bs4 import BeautifulSoup

import PostSolution
import Secret

headers = {
    "User-Agent": Secret.USER_AGENT,
    "Cookie": Secret.COOKIE
}

conn = pymysql.connect(host="localhost", user=Secret.DB_USER, password=Secret.DB_PASSWORD, database="douban_hype", port=3306,
                       charset="utf8mb4")
cursor = conn.cursor()

group_list = ['a-soul']


async def insert_elite(paras):
    # para (topic_id,topic_name,group_id,member_id,topic_comments,topic_like,topic_collect,topic_share)
    sql = "REPLACE into elite_list (topic_id,topic_name,group_id,member_id,topic_comments,topic_like,topic_collect,topic_share) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)"
    try:
        cursor.executemany(sql, paras)
        conn.commit()
        print("insert finished")
    except:
        traceback.print_exc()
        conn.rollback()


async def insert_member(paras):
    sql = "REPLACE into member_list VALUES (%s,%s)"
    try:
        cursor.executemany(sql, paras)
        conn.commit()
        print("insert finished")
    except:
        traceback.print_exc()
        conn.rollback()


def has_title(tag):
    return tag.has_attr('title')


async def get_reaction(session, topic_id):
    success = None
    while not success:
        url = "https://www.douban.com/group/topic/{0}/".format(topic_id)
        await sleep(0.5)
        try:
            async with session.get(url, headers=headers) as response:  # 获取网页HTML
                is_denied = response.url.__str__()[:33] == "https://www.douban.com/misc/sorry"  # 判断是否被拒绝
                if not is_denied:  # 如果没有被拒绝，则获取网页元素
                    count = await response.text()
                    soup = BeautifulSoup(count, "lxml")
                    like_element = soup.find('a', attrs={"class": "react-add react-btn"})
                    like_element = like_element.find("span", attrs={"class": "react-num"}).get_text()  # 可能为空
                    collect_element = soup.find("a", attrs={"class": "lnk-doulist-add collect-add"})
                    collect_element = collect_element.find("span", attrs={"class": "react-num"}).get_text()  # 可能为空
                    share_element = soup.find("span", attrs={"class": "rec-num"})  # 可能为None
                if is_denied:  # 如果被拒绝，则提交验证码，再次循环
                    await PostSolution.main()
                    await sleep(2)
                    continue

            like_num = int(like_element) if like_element != "" else 0
            collect_num = int(collect_element) if collect_element != "" else 0
            share_num = int(share_element.get_text()) if share_element is not None else 0
            success = True
            return like_num, collect_num, share_num
        except:
            traceback.print_exc()
            success = None


async def get_elite(session, group_id):
    success = None
    next_start = 0
    while not success:  # 循环结构
        url = "https://www.douban.com/group/{0}/discussion?start={1}&type=elite".format(group_id, next_start)
        await sleep(0.5)
        topicParas = []
        memberParas = []
        try:
            async with session.get(url, headers=headers) as response:  # 获取网页HTML
                is_denied = response.url.__str__()[:33] == "https://www.douban.com/misc/sorry"  # 判断是否被拒绝
                if not is_denied:  # 如果没有被拒绝，则获取网页元素
                    count = await response.text()
                    soup = BeautifulSoup(count, "lxml")
                    comment_elements = soup.find_all("td", attrs={"class": "r-count"})[1:]
                    time_elements = soup.find_all("td", attrs={"class": "time"})
                    topic_elements = soup.find_all(has_title)
                    auth_elements = soup.find_all(href=re.compile("people"))
                    comment_flag = soup.select("#content > h1")
                    is_empty = len(comment_elements) <= 0
                    is_denied = len(comment_flag) <= 0
                if is_denied:  # 如果被拒绝，则提交验证码，再次循环
                    await PostSolution.main()
                    await sleep(2)
                    continue

                if not is_empty:
                    for index, comment in enumerate(comment_elements):  # 判断评论是否过期并计数
                        topic_name = topic_elements[index].get_text().strip()
                        topic_time = datetime.datetime.strptime("21-" + time_elements[index].get_text().strip(),
                                                                "%y-%m-%d %H:%M")
                        if (datetime.datetime.now() - topic_time).days > 14:
                            print("out time")
                            continue
                        author_name = auth_elements[index].get_text().strip()
                        topic_id = eval(topic_elements[index].get('href')[35:-1])
                        reaction_num = await get_reaction(session, topic_id)
                        print(reaction_num)
                        author_id = auth_elements[index].get('href')[30:-1]
                        count = comment.get_text()
                        if count.isdigit() or count == '':
                            comment_count = eval(count if count != "" else "0")
                            topicParas.append((topic_id, topic_name, group_id, author_id, comment_count) + reaction_num)
                            if author_name != "[已注销]":
                                memberParas.append((author_id, author_name))
                    print(next_start)

                    # 获取下一页地址
                    next_start_element = soup.select("#content > div > div.article > div.paginator > span.next > a")
                    is_end = len(next_start_element) <= 0
                else:
                    is_end = True

            if not is_end:  # 判断是否还有下一页，如果没有，结束循环
                next_start = next_start_element[0].get('href')
                next_start = next_start[next_start.index("=") + 1:]
            else:
                success = True
        except:
            traceback.print_exc()
            success = None
        await insert_member(memberParas)
        await insert_elite(topicParas)


async def main():
    async with aiohttp.ClientSession() as session:
        for group_id in group_list:
            await get_elite(session, group_id)


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
