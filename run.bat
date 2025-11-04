@echo off

REM 1. Ativa o ambiente virtual
REM Altere 'venv' se o nome da sua pasta de ambiente for diferente
call venv\Scripts\activate.bat

echo Ambiente virtual ativado.

REM 2. Executa o projeto
python main.py

REM Opcional: Para manter a janela aberta após a execução
pause