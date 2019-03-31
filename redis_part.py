# импорт redis
import redis
from datetime import datetime
import json

# параметры подключения к redis
redis_host = "localhost"
redis_port = 6379
redis_password = ""


# персоны из первой лабораторной работы
docs = [{'person_id': 0,
         'first name': 'Mike',
         'last name': 'Wazowski',
         'workplace': 'Monsters, Inc.',
         'phone number': '+19039615830',
         'birthday': '1996.5.13',
         'hobby': 'Scaring',
         'Favorite color': 'Green'},
         {'person_id': 1,
         'first name': 'John',
         'last name': 'Connor',
         'workplace': '',
         'phone number': '+19031828330',
         'birthday': '1970.3.23',
         'hobby': 'Playing guitar',
         'Favorite color': 'Black'},
         {'person_id': 2,
         'first name': 'Freddie',
         'last name': 'Mercury',
         'workplace': 'Queen',
         'phone number': '+19641920593',
         'birthday': '1946.9.5',
         'hobby': 'Signing',
         'Favorite color': 'Yellow'},
         {'person_id': 3,
         'first name': 'Tony',
         'last name': 'Stark',
         'workplace': 'Stark Industries',
         'phone number': '+19632018539',
         'birthday': '1970.5.29',
         'hobby': 'Inventing',
         'Favorite color': 'Purple'}]


def hello_redis():
    # Подключение
    try:
        r = redis.StrictRedis(host=redis_host, port=redis_port, password=redis_password, decode_responses=True)
        # Добавим их по порядку по id
        for person in docs:
            r.set(person["person_id"], json.dumps(person))
        # получим объект с ключом 0
        msg = json.loads(r.get(0))
        # изменим значение
        msg['first name'] = 'Tom'
        # обновим в БД
        r.set(0, json.dumps(msg))
        # удаление объекта с ключом 2
        r.delete(2)
        # итерирование по всем значениям
        for key in r.keys():
            print(r.get(key))
    except Exception as e:
        print(e)

if __name__ == '__main__':
    hello_redis()