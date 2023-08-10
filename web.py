import tornado.ioloop
import tornado.web
import mysql.connector
import base
import json
import zjgtjy

CONFIG = {}
class UpdateHandler(tornado.web.RequestHandler):
    def get(self):
        zjgtjy.run(CONFIG)
        self.write("ok")

class MainHandler(tornado.web.RequestHandler):
    def get(self):
        conn = base.getDbConnection(CONFIG)
        sql = r"""select * from zjgtjy3_dist"""
        cursor = conn.cursor()
        cursor.execute(sql)
        r = []
        for row in cursor.fetchall():
            d = dict((cursor.description[i][0], value) for i, value in enumerate(row))
            r.append(d)
        
        cursor.close()
        conn.close()
        data = json.dumps(r, ensure_ascii=False)
        # print(data)
        self.write(data)

def make_app():
    return tornado.web.Application([
            (r"/", MainHandler),
            (r"/update", UpdateHandler)
        ]
    )

if __name__ == "__main__":
    CONFIG = base.loadConfig()
    app = make_app()
    app.listen(8888)
    tornado.ioloop.IOLoop.current().start()
