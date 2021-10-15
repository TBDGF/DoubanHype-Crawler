import traceback

import requests
from bs4 import BeautifulSoup
import aiohttp
import asyncio
from asyncio import sleep
import ImageOCR
import Secret

headers = {
    "User-Agent": Secret.USER_AGENT,
    "Cookie": Secret.COOKIE,
    "Host": "www.douban.com",
    "Origin": "https://www.douban.com",
    "Referer": "https://www.douban.com/misc/sorry?original-url=https://www.douban.com/group/",
    "Content-Type": "application/x-www-form-urlencoded"
}

img_headers = {
    "User-Agent": Secret.USER_AGENT,
    "Cookie": Secret.COOKIE,
}

form = {
    "ck": "41WX",
    "captcha-solution": "church",
    "captcha-id": "3VjSrtv7u8GHJ5VFG0j6qJx0:en",
    "original-url": "https://www.douban.com/group/"
}


async def fetch(session):
    print("start post")
    url = "https://www.douban.com/misc/sorry"
    is_access = None
    while not is_access:  # 循环，上一次的结果在下一次循环时提交
        try:
            await sleep(0.3)
            async with session.post(url, headers=headers, data=form) as response:

                if response.url.__str__()[:33] != "https://www.douban.com/misc/sorry":
                    is_access = True

                if not is_access:
                    text = await response.text()
                    soup = BeautifulSoup(text, "lxml")
                    img_elements = soup.find_all("img",attrs={"alt": "captcha"})
                    img_src = img_elements[0].get('src')  # 获取图片地址
                    img_id = img_src[39:]  # 获取图片id

                    with open("list/captcha.jfif", 'wb+') as f:  # 保存图片到本地
                        res = requests.get(img_src, headers=img_headers)
                        f.write(res.content)
                        f.close()
                    img = ImageOCR.Img()  # 执行OCR
                    solution = img.deocr()

                form['captcha-solution'] = solution
                form['captcha-id'] = img_id
                print(form)
        except:
            print("post failed")
            traceback.print_exc()
            is_access = None
    print("access")


async def main():
    async with aiohttp.ClientSession() as session:
        await fetch(session)


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
