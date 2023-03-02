import mysql.connector

def db_connect(mysql_server,dbname,mysql_username,mysql_passwd,mysql_port=3306):
        # mysql_server = 'localhost'
        # mysql_username = 'moratel'
        # mysql_passwd = 'Moratel.789'
        # mysql_username = 'root'
        # mysql_passwd = ''
        mysql_db = dbname
        global db, cursor
        db = mysql.connector.connect(host=mysql_server, user=mysql_username, password=mysql_passwd, database=mysql_db,port=mysql_port)
        db.autocommit=True
        cursor = db.cursor(dictionary=True, buffered=True)

def db_status():
        return db.is_connected()

def db_reconnect():
        db.reconnect(attempts=1, delay=10)

def db_query(query):
        try:
                cursor.execute(query)
                db.commit()
                return True
        except (mysql.connector.Error, mysql.connector.Warning) as e:
                return False

def db_fetch(query):
        try:
                rows=None
                cursor.execute(query)
                rows = cursor.fetchall()
                db.commit()
                return rows
        except (mysql.connector.Error, mysql.connector.Warning) as e:
                return False

def db_fetchone(query):
        try:
                cursor.execute(query)
                rows = cursor.fetchone()
                db.commit()
                return rows
        except (mysql.connector.Error, mysql.connector.Warning) as e:
                return False

def db_single(tablename,column,key,val):
        try:
                cursor.execute("select "+column+" from "+tablename+" where "+key+" = '"+val+"'")
                rows = cursor.fetchone()
                db.commit()
                if rows:
                        return rows[column]
                # db.commit()
                return None
        except (mysql.connector.Error, mysql.connector.Warning) as e:
                return None

def db_count(query):
        try:
                cursor.execute(query)
                rows = cursor.fetchall()
                count=0
                for row in rows:
                        count = count + 1
                db.commit()
                return count
        except (mysql.connector.Error, mysql.connector.Warning) as e:
                return False
def db_close():
        try:
               
                db.close()
                return True
        except (mysql.connector.Error, mysql.connector.Warning) as e:
                return False

class MySql:
        # def __init__(self):

        def __init__(self,dbname):
                self.mysql_server = 'localhost'
                self.mysql_username = 'hariff'
                self.mysql_passwd = 'hariff123'
                self.mysql_db = dbname
                # self.db, 
                # self.cursor
                self.db = mysql.connector.connect(self.mysql_server, self.mysql_username, self.mysql_passwd, self.mysql_db)
                self.cursor = self.db.cursor(dictionary=True, buffered=True)

        def db_query(self,query):
                try:
                        self.cursor.execute(query)
                        self.db.commit()
                        return True
                except (mysql.connector.Error, mysql.connector.Warning) as e:
                        return False

        def db_fetch(self,query):
                try:
                        rows=None
                        self.cursor.execute(query)
                        rows = self.cursor.fetchall()
                        self.db.commit()
                        return rows
                except (mysql.connector.Error, mysql.connector.Warning) as e:
                        return False

        def db_fetchone(self,query):
                try:
                        self.cursor.execute(query)
                        rows = self.cursor.fetchone()
                        self.db.commit()
                        return rows
                except (mysql.connector.Error, mysql.connector.Warning) as e:
                        return False

        def db_single(self,tablename,column,key,val):
                try:
                        self.cursor.execute("select "+column+" from "+tablename+" where "+key+" = '"+val+"'")
                        rows = self.cursor.fetchone()
                        self.db.commit()
                        if rows:
                                return rows[column]
                        return None
                except (mysql.connector.Error, mysql.connector.Warning) as e:
                        return None

        def db_count(self,query):
                try:
                        self.cursor.execute(query)
                        rows = self.cursor.fetchall()
                        count=0
                        for row in rows:
                                count = count + 1
                        self.db.commit()
                        return count
                except (mysql.connector.Error, mysql.connector.Warning) as e:
                        return False
        def db_close(self):
                try:
                
                        self.db.close()
                        return True
                except (mysql.connector.Error, mysql.connector.Warning) as e:
                        return False
