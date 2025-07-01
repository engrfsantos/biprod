# Este projeto serviu de ETL para migração de dados do ambiente ORACLE para ambiente POSTGRES
#Abrir Pasta do projeto BIPROD

## Criar o ambiente virtual
python -m venv ./venv

## Para ativar o ambiente virtual
## (no windows a ativação é feita por .ps1)

.\venv\Scripts\Activate.ps1 


# pode ser necessário rodar o update do pip
pip install update pip

## se criar o ambiente virtual diretamente no windows, ou seja, sem atach ## no ambiente shell de um docker, e apresente mensagem de erro abaixo,
## digitar em seguida: 
Set-ExecutionPolicy Unrestricted

# Ou Tente
Set-ExecutionPolicy -Scope CurrentUser

## responda: 
Unrestricted

## Alterar o compilador python para o compilador do venv
menu-> view->command pallet->Python Selected Interpreter 
Selecione o interpretador dentro de .\venv\Script\python.exe

## Ou responda OK para o VSCode assumir o compilador PYTHON no diretório do VENV, esta mensagem aparece no canto inferior do VSCode logo ao criar o ambiente VENV

instalar as duas bibliotecas
(venv) PS C:\BITSI> pip install psycopg2 
(venv) PS C:\BITSI> pip install oracledb

#registre as variáveis de ambiente no windows para oracle
ORACLE_USR = usuario
ORACLE_IPPUSER = ip:porta/servico
ORACLE_PSW = senha

POSTGRES_DB = banco 
POSTGRES_USR = usuario
POSTGRES_PSW = senha
POSTGRES_IP = ip
POSTGRES_PORT = porta



git init
git config --global user.email "eng.rfsantos@gmail.com"
git config --global user.name "engrfsantos"
git commit -m "first commit"
git add .
git branch -M main
git remote add origin https://github.com/engrfsantos/biprod.git
git push -u origin main
