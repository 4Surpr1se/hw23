import os

from flask import Flask, request, abort

app = Flask(__name__)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")


class QueryExeption(Exception):
    pass


class Query:
    #  TODO Если один запрос, то из cache в reply_data перенести
    def __init__(self, file_out, query_count=0, file_in='cache.txt'):
        self.file_in = file_in
        self.file_out = file_out
        self.query_count = query_count

    # def file_gen(self, file=None, is_strip=True):
    def file_gen(self, file_name=None):
        if file_name is None:
            file_name = self.file_out
        file = open(f'./data/{file_name}', "r")
        for row in file:
            yield row.strip()
        file.seek(0)
        file.close()
        # if file is None: ПОЧЕМУ ЭТО НЕ РАБОТАТ, НО КАК ТОЛЬКО GEN И LIMIT FILE В ОТДЕЛЬНОМ ФАЙЛЕ
        # ТО ОНО РАБОТАЕТ НОРМАЛЬНО  ШПГЦТЦДЛАР
        #     file = self.file_out
        #     with open(f'./data/{file}', 'r') as f:
        #         while True:
        #             try:
        #                 if is_strip:
        #                     line = next(f)
        #                     yield line.strip()
        #                     print('+')
        #                 else:
        #                     yield next(f)
        #             except StopIteration as e:
        #                 break
        #                 return ''

    def file_limit(self, value):
        value = int(value)
        reply_list = []
        gen = self.file_gen(self.file_out)
        print(gen)
        with open(f'./data/{self.file_in}', 'a') as file:
            for counter, i in enumerate(gen, start=1):
                reply_list.append(i)
                print(counter)
                if counter == value:
                    break
            file.writelines(line + '\n' for line in reply_list)
        self._file_changer()

    def wrapper(self, func=None, value=None):
        c = self.file_gen()
        not_filtered_info = []
        for counter, i in enumerate(c, start=1):
            if counter % 10 == 0:
                reply = func(list_=not_filtered_info, value=value)
                not_filtered_info.clear()
                with open(f'./data/{self.file_in}', 'a') as return_f:
                    return_f.writelines(line + '\n' for line in reply)
            not_filtered_info.append(i)  # надо убрать /n в конце
        reply = func(list_=not_filtered_info, value=value)
        with open(f'./data/{self.file_in}', 'a') as return_f:
            return_f.writelines(line + '\n' for line in reply)

    def file_filter(self, list_=None, value=None, *args, **kwargs):
        return list(filter(lambda v: value in v, list_))

    def file_map(self, value=None, list_=None, *args, **kwargs):
        return list(map(lambda v: v.split(" ")[int(value)], list_))

    def file_unique(self):
        """здесь можно было сделать и для больших файлов, как в file_map или file_filter,
         разбив файл на несколько частей и поочередно сравнивать их,
        но не думаю что в задании это вообще подразумевалось, поэтому сделаю по обычному"""
        set_gen = set(self.file_gen())
        with open(f'./data/{self.file_in}', 'a') as file:
            file.writelines(line + '\n' for line in set_gen)
        self._file_changer()

    def file_sort(self, value):
        gen = list(self.file_gen())
        print(value)

        if value == 'asc':
            print('not reverse')
            reply = sorted(gen)
        else:
            print('reverse')
            reply = sorted(gen, reverse=True)


        with open(f'./data/{self.file_in}', 'a') as file:
            file.writelines(line + '\n' for line in reply)
        self._file_changer()

    def file_changer(self):
        self._file_changer()

    def _file_changer(self):
        self.file_out = self.file_in
        self.file_in = 'reply_data.txt'
        self.query_count += 1

    def _reply_file(self):
        if self.query_count == 2:
            return 'reply_data.txt'
        return 'cache.txt'

    def reply(self):
        reply = '<br>'.join(self.file_gen(file_name=self._reply_file()))  # , is_strip=False))
        self.__clear_files()
        return reply

    def __clear_files(self):

        with open(f'./data/cache.txt', 'w') as file:
            pass
        if self.query_count == 2:
            with open(f'./data/reply_data.txt', 'w') as file:
                pass


@app.route("/perform_query")
def perform_query():
    # получить параметры query и file_name из request.args, при ошибке вернуть ошибку 400
    # проверить, что файла file_name существует в папке DATA_DIR, при ошибке вернуть ошибку 400
    # с помощью функционального программирования (функций filter, map), итераторов/генераторов сконструировать запрос
    # вернуть пользователю сформированный результат
    cmd_list = []
    value_list = []
    try:
        request_args = request.args
        for k, v in request_args.items():
            if k not in ['cmd1', 'value1', 'cmd2', 'value2', 'file_name']:
                raise QueryExeption  # error 400 вернуть
            # exec("%s = %d" % (k,v))
        cmd_list.extend([request_args.get('cmd1'), request_args.get('cmd2')])
        value_list.extend([request_args.get('value1'), request_args.get('value2')])
        file_name = request_args['file_name']
    except QueryExeption as e:
        abort(501, e)  # err 404 zabil kak

    try:
        with open(f'./data/{file_name}') as f:
            pass
    except FileNotFoundError as e:
        abort(401, e)
    query = Query(file_out=file_name)

    if 'filter' in cmd_list:
        query.wrapper(func=query.file_filter, value=value_list[cmd_list.index('filter')])
        query.file_changer()  # костыль* (так говорят крутые, да?)

    if 'map' in cmd_list:
        query.wrapper(func=query.file_map, value=value_list[cmd_list.index('map')])
        query.file_changer()

    if 'unique' in cmd_list:
        query.file_unique()

    if 'limit' in cmd_list:
        query.file_limit(value_list[cmd_list.index('limit')])

    if 'sort' in cmd_list:
        query.file_sort(value_list[cmd_list.index('sort')])

    return query.reply()
# def func(req):
# for k, v in req.items():
#   if k not in ['cmd1','value1','cmd2', 'value2', 'file_name']:
#     raise QueryExeption #  error 400 вернуть
#   exec("%s = %d" % (k,v))


if __name__ == '__main__':
    app.run()
