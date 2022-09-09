import urllib.request
import requests
from bs4 import BeautifulSoup
import os


def get_content(url):
    links = {}
    soup = BeautifulSoup(requests.get(url).text, 'html.parser')
    items = soup.find_all('td')
    link = (str(items[23]).split('"')[-1].split(' ')[0].split('>')[-1])
    return link


def d_download_file(url, out_dir):
    link = get_content(url)
    file_name = link.split('/')[-1]
    print(file_name)
    file_path_name = os.path.join(out_dir, file_name)
    print(link)
    print(file_path_name)
    # urllib.request.urlretrieve(link, file_path_name)                              !!!!!!!!!!!!!!!!!!!!!!!!!!!


def d_download_file_main():
    out_dir = 'C:/Users/ngzirishvili/PycharmProjects/pythonProject9/data'
    url = 'https://www.nalog.gov.ru/opendata/7707329152-rsmp/'
    d_download_file(url, out_dir)


if __name__ == '__main__':
    out_dir = 'C:/Users/ngzirishvili/PycharmProjects/pythonProject9/data'
    url = 'https://www.nalog.gov.ru/opendata/7707329152-rsmp/'
    d_download_file(url, out_dir)
