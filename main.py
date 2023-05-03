import os
import sqlite3
import requests
from selenium import webdriver
import multiprocessing
from datetime import datetime
import time
import random
from bs4 import BeautifulSoup
from fake_useragent import UserAgent
import json


def create_db():
    """Создание БД"""
    conn = sqlite3.connect('profiles.db')
    cursor = conn.cursor()
    cursor.execute('''CREATE TABLE IF NOT EXISTS `Cookie Profile` (
                    `id` INTEGER PRIMARY KEY AUTOINCREMENT,
                    `created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
                    `cookie` TEXT,
                    `last_run` TIMESTAMP,
                    `run_count` INTEGER DEFAULT 0
                );''')
    for i in range(15):
        cursor.execute('''INSERT INTO "Cookie Profile" (created_at) VALUES (datetime('now'));''')
    conn.commit()
    conn.close()


def get_news_links():
    """Функция для получения новостных ссылок"""
    url = 'https://news.google.com'
    response = requests.get(url)
    soup = BeautifulSoup(response.content, 'html.parser')
    links = []
    if response.status_code == 200:
        for link in soup.find_all('a'):
            href = link.get('href')
            if href and href.startswith('./articles/'):
                full_url = url + href[1:]
                links.append(full_url)
    return links


def save_db(cookie, id):
    """Сохранение данных в БД"""
    conn = sqlite3.connect('profiles.db')
    cursor = conn.cursor()
    now = datetime.now()
    cursor.execute("UPDATE `Cookie Profile` SET cookie = ?, last_run = ?, run_count = run_count + 1 WHERE id = ?", (cookie, now, id))
    conn.commit()
    conn.close()


def run_session(cookie):
    """Запуск одной сессии"""
    user_agent = UserAgent()
    chrome_options = webdriver.ChromeOptions()
    chrome_options.add_argument(f"user-agent={user_agent.random}")
    driver_path = os.path.join(os.getcwd(), "chromedriver", "chromedriver.exe")
    driver = webdriver.Chrome(
        executable_path=driver_path,
        options=chrome_options
        )
    driver.get('https://news.google.com')
    if cookie[1]:
        for cook in cookie[1]:
            driver.add_cookie(cook)
    driver.refresh()
    new_links = get_news_links()
    new_cookie = json.dumps(driver.get_cookies())
    save_db(new_cookie, cookie[0])
    driver.get(random.choice(new_links))
    time.sleep(random.randint(2, 5))
    driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
    time.sleep(random.randint(2, 5))
    driver.quit()


def run_multiprocessing():
    """Запуск нескольких процессов"""
    profiles = []
    id_ = []
    conn = sqlite3.connect('profiles.db')
    cursor = conn.cursor()
    cursor.execute('''SELECT id, cookie FROM "Cookie Profile"''')
    rows = cursor.fetchall()
    for row in rows:
        id_.append(row[0])
        if row[1]:
            profiles.append(json.loads(row[1]))
        else:
            profiles.append(row[1])

    with multiprocessing.Pool(processes=min(5, len(profiles))) as pool:
        pool.map(run_session, zip(id_, profiles))


if __name__ == '__main__':
    create_db()
    while True:
        run_multiprocessing()
        time.sleep(86400)
