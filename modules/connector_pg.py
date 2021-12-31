import psycopg2

class Bancosql:
    usuario, senha, host, banco = "", "", "", ""
    
    def __init__(self, usuario, senha, host, banco):
        # """Construtor da classe interface_db

        # Args:
        #     usuario (string): usuario para conexao ao banco
        #     senha (string): senha para acesso ao banco
        #     host (string): string contendo o endereco do host
        #     banco (string): string contendo o nome do banco
        # """
        try:
            self.usuario = usuario
            self.senha = senha
            self.host = host
            self.banco = banco
        except Exception as e:
            print(str(e))
    
    def conectar(self):
        try:
            con = psycopg2.connect(user=self.usuario, password=self.senha, host=self.host, database=self.banco)
            cursor = con.cursor()
            return con, cursor
        except Exception as e:
            print(str(e))
            
    def desconectar(self, con, cursor):
        try:
            cursor.close()
            con.commit()
            con.close()
        except Exception as e:
            print(str(e))
    
    def buscar(query):
        try:
            con = psycopg2.connect(user='postgres', password='admin', host='127.0.0.1', database='oldtech')
            cursor = con.cursor()
            cursor.execute(query)
            return cursor.fetchall() 
        except Exception as e:
            print(e)
        finally:
            cursor.close()
            con.close()
            
    def executar(query):
        try:
            con = psycopg2.connect(user='postgres', password='admin', host='127.0.0.1', database='oldtech')
            cursor = con.cursor()
            cursor.execute(query)
            cursor.close()
            con.commit()
            con.close()
        except Exception as e:
            print(e)
            
