import unittest
import requests, json, random
import time
from bson.objectid import ObjectId
from bson import json_util

# with open('mm.txt', encoding='unicode') as f:
#     print(f.read())
url = 'https://www.google.com.hk/search?newwindow=1&safe=strict&hl=zh-CN&ei=_RO6WtqmPMmv0gTegrPwBQ&hotel_occupancy=&q=%E4%BC%A6%E6%95%A6%E5%81%87%E6%97%A5%E9%85%92%E5%BA%97+-+%E6%96%AF%E7%89%B9%E6%8B%89%E7%89%B9%E7%A6%8F%E5%B8%82&oq=%E4%BC%A6%E6%95%A6%E5%81%87%E6%97%A5%E9%85%92%E5%BA%97+-+%E6%96%AF%E7%89%B9%E6%8B%89%E7%89%B9%E7%A6%8F%E5%B8%82&gs_l=psy-ab.12...0.0.0.7158898.0.0.0.0.0.0.0.0..0.0....0...1..64.psy-ab..0.0.0....0.ZfhGq_J7uhs'
# url = 'http://www.baidu.com'
headers={'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/65.0.3325.181 Safari/537.36',
         'referer': 'https:/www.google.com.hk/',
        ':scheme': 'https',
        'accept-encoding': 'gzip, deflate, br',
        'accept-language': 'zh-CN, zh',
        'q': '0.9'
}
content = requests.get(url, headers=headers)
print(content)