FROM alpine:latest

# Instalar dependências
RUN apk add --no-cache \
    python3 \
    py3-pip

# Instalar bibliotecas Python necessárias
COPY requirements.txt /app/requirements.txt
RUN pip3 install --break-system-packages -r /app/requirements.txt

# Copiar o script para o diretório de trabalho
COPY infobarbank.py /app/infobarbank.py

# Definir o diretório de trabalho
WORKDIR /app

# Comando para executar o script
CMD ["python3", "infobarbank.py"]
