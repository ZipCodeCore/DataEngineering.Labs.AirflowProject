from bs4 import BeautifulSoup
import requests
import prettify
# import lxml
import csv

stats = requests.get('https://www.pro-football-reference.com/play-index/tiny.fcgi?id=yNqbP').text

soup = BeautifulSoup(stats, 'lxml')

file = open('football.csv', 'w')

info = soup.find('table', class_="sortable stats_table")

# 'csv_results'


print(info)

