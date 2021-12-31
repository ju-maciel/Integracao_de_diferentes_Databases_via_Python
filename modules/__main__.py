# Atividade feita no curso de data engineer da SoulCode Academy desenvovida por:
# Juliana Maciel
# Sandi Ourique
# Aurelia Bagagim

from modules.inserts import Insert
from modules.connector import Bancosql
from modules.connector_cassandra import Cassandra
import os

def menu():
    os.system('cls')
    print("#--------------------------------------------------------#")
    print("#-----------------Siga a sequência do menu---------------#")
    print("#--------------------------------------------------------#")
    print("[1] - Popular Banco de dados Matriz Oldtech")
    print("[2] - Mostrar dados da  Matriz Oldtech")
    print("[3] - Popular Banco de dados Filial Oldtech")
    print("[4] - Mostrar dados Filial Oldtech")
    print("[5] - Transportar dados da Filial para a Matriz")
    print("[6] - Para Sair do sistema")
    
    
    try:
        escolha = int(input("Siga a sequência do menu: "))
    except Exception as e:
        print("Opção inválida.")
        menu()
    
    if escolha == 1:
        Insert.popular_nosql()
        menu()
    elif escolha == 2:
        Insert.mostrar_matriz()
        menu()      
        
    elif escolha == 3:
        Insert.popular_sql()
        menu()
    elif escolha == 4:
        Insert.mostrar_filial()
        menu()
    elif escolha == 5:
        Insert.popular_nosql_sql()
        menu()
    elif escolha == 6:
        exit()
    else:
        print("Opção Inválidação!")
        menu()
    

if __name__ == "__main__":
    
    menu()