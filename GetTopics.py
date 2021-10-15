import asyncio
import datetime
import re
import traceback
from asyncio import sleep
from dateutil.parser import parse
from dateutil.relativedelta import relativedelta
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


async def insert_member(paras):
    sql = "REPLACE into member_list VALUES (%s,%s)"
    try:
        cursor.executemany(sql, paras)
        conn.commit()
        print("insert finished")
    except:
        traceback.print_exc()
        conn.rollback()


async def insert_topic(paras):
    # para (topic_id,topic_name,group_id,member_id,topic_comments)
    sql = "REPLACE into topic_list (topic_id,topic_name,group_id,member_id,topic_comments) VALUES (%s,%s,%s,%s,%s)"
    try:
        cursor.executemany(sql, paras)
        conn.commit()
        print("insert finished")
    except:
        traceback.print_exc()
        conn.rollback()


async def insert_current(paras):
    # para (topic_id,topic_name,group_id,member_id,topic_comments)
    sql = "REPLACE into current_list (topic_id,topic_name,group_id,member_id,topic_comments) VALUES (%s,%s,%s,%s,%s)"
    try:
        cursor.executemany(sql, paras)
        conn.commit()
        print("insert finished")
    except:
        traceback.print_exc()
        conn.rollback()


async def clear_current():
    # para (topic_id,topic_name,group_id,member_id,topic_comments)
    sql = "delete from current_list"
    try:
        cursor.execute(sql)
        conn.commit()
        print("clear finished")
    except:
        traceback.print_exc()
        conn.rollback()


async def get_group_id():
    sql = "SELECT group_id FROM group_list"
    result = []
    try:
        cursor.execute(sql)
        origin = cursor.fetchall()
        for item in origin:
            result.append(item[0])
        print(result)
    except:
        traceback.print_exc()
        conn.rollback()
    return result


def has_title(tag):
    return tag.has_attr('title')


async def get_topics(session, group_id, is_current=None):
    success = None
    next_start = 0
    while not success:  # 循环结构
        url = "https://www.douban.com/group/{0}/discussion?start={1}".format(group_id, next_start)
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
                    is_empty = len(comment_elements) <= 0 or len(time_elements) <= 0
                    is_denied = len(comment_flag) <= 0
                if is_denied:  # 如果被拒绝，则提交验证码，再次循环
                    await PostSolution.main()
                    await sleep(2)
                    continue

                if not is_empty:
                    for index, comment in enumerate(comment_elements):  # 判断评论是否过期并计数
                        topic_time = datetime.datetime.strptime("21-" + time_elements[index].get_text().strip(),
                                                                "%y-%m-%d %H:%M")
                        time_out = None
                        if (datetime.datetime.now() - topic_time).days > 14:
                            time_out = True
                            print("out time")
                            break
                        topic_name = topic_elements[index].get_text().strip()
                        author_name = auth_elements[index].get_text().strip()
                        topic_id = eval(topic_elements[index].get('href')[35:-1])
                        author_id = auth_elements[index].get('href')[30:-1]
                        count = comment.get_text()
                        if count.isdigit() or count == '':
                            num = eval(count if count != "" else "0")
                            topicParas.append((topic_id, topic_name, group_id, author_id, num))
                            if author_name != "[已注销]":
                                memberParas.append((author_id, author_name))
                    print(next_start)

                    # 获取下一页地址
                    next_start_element = soup.select("#content > div > div.article > div.paginator > span.next > a")
                    is_end = len(next_start_element) <= 0
                else:
                    is_end = True

            if not is_end and not time_out:  # 判断是否还有下一页，如果没有，结束循环
                next_start = next_start_element[0].get('href')
                next_start = next_start[next_start.index("=") + 1:]
            else:
                success = True
        except:
            traceback.print_exc()
            success = None
        await insert_topic(tuple(topicParas))
        await insert_current(tuple(topicParas))
        await insert_member(tuple(memberParas))


async def main():
    async with aiohttp.ClientSession() as session:
        group_list = await get_group_id()  # 从数据库读取小组列表
        # await clear_current()
        # await PostSolution.main()
        start = True
        for index, group_id in enumerate(group_list):
            if start or group_id == "asoul":
                start = True
            else:
                continue
            await get_topics(session, group_id)
        print("fetch over")


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
