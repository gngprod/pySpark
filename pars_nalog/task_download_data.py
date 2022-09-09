from dataclasses import dataclass
import urllib.request
import requests
from bs4 import BeautifulSoup
import os
import zipfile
import shutil
import glob


def get_content(url):
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
    urllib.request.urlretrieve(link, file_path_name)
    d_unzip(out_dir)


def d_unzip(out_dir):
    archive = glob.glob(f'{out_dir}*.zip')
    print(out_dir)
    print(archive[0])
    shutil.rmtree(f'{out_dir}/out_file', ignore_errors=True)
    fantasy_zip = zipfile.ZipFile(archive[0])
    fantasy_zip.extractall(f'{out_dir}/out_file/')
    fantasy_zip.close()


def d_download_file_main():
    out_dir = '/home/ubuntuos/nalog/data/'
    url = 'https://www.nalog.gov.ru/opendata/7707329152-rsmp/'
    d_download_file(url, out_dir)


if __name__ == '__main__':
    out_dir = '/home/ubuntuos/nalog/data/'
    url = 'https://www.nalog.gov.ru/opendata/7707329152-rsmp/'
    d_download_file(url, out_dir)
