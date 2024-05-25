FROM python:3.8-slim 
RUN pip install psycopg2-binary requests pandas 
COPY script.py /app/ 
WORKDIR /app 
CMD ["python3", "script.py"] 

# en la terminal correr: docker build -t projecto . para crear la imagen
# luego escribir: docker run projecto para correr el contenedor
