from cassandra.cluster import Cluster
import pandas as pd

class Cassandra:           
    def __init__(self,keyspace:str):
        self.cluster = Cluster()
        self.session = self.cluster.connect(keyspace)

    def inserir(self,tipo:int=0,familia:str='', informacao=''):
            for i in  informacao:
                chave = list(i.keys())
                valor = list(i.values())
                query = f"INSERT INTO {familia} ({','.join(chave)}) VALUES ({','.join(valor)});"
                # print(query)
                self.session.execute(query)
    
    def buscar(self, familia:str=''):
        query = f"SELECT * FROM {familia};"
        dados = self.session.execute(query)
        dados_f = pd.DataFrame(dados)
        print(dados_f) 

