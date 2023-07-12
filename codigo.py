import sys
import nltk
from selenium import webdriver
from selenium.webdriver.common.by import By
import time
import sqlite3
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from unidecode import unidecode
from collections import Counter
from mpi4py import MPI
import csv

def obtener_palabras_clave(texto):
    texto = unidecode(texto)
    stopwords_es = set(stopwords.words('spanish'))
    palabras = word_tokenize(texto)
    palabras_clave = [palabra for palabra in palabras if palabra.lower() not in stopwords_es and palabra.isalpha()]
    return palabras_clave


if __name__ == '__main__':

    sys.stdout.reconfigure(encoding='utf-8')

    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    hostname = MPI.Get_processor_name()

    if rank == 0:
        print("Hola soy la pc: ", hostname, ". Yo extraigo los comentarios")

        conn = sqlite3.connect('publicaciones.db')
        c = conn.cursor()
        # c.execute('''CREATE TABLE IF NOT EXISTS comentarios(publicacion TEXT,usuario TEXT,comentario TEXT)''')
        c.execute("SELECT * FROM publicaciones LIMIT 5")
        publicaciones = c.fetchall()

        url = "https://mbasic.facebook.com"
        browser = webdriver.Chrome()
        browser.get(url)
        browser.find_element(By.CSS_SELECTOR, '[name="email"]').send_keys("garrotasos7@gmail.com")
        browser.find_element(By.CSS_SELECTOR, '[name="pass"]').send_keys("DanielL651")
        browser.find_element(By.CSS_SELECTOR, '[name="login"]').click()

        for publicacion in publicaciones:
            browser.get(publicacion[5])

            while True:
                aux = True
                # Comentarios
                try:
                    bodyComentarios = browser.find_element(By.ID, "m_story_permalink_view")
                    comentarios = bodyComentarios.find_elements(By.XPATH, "./div[2]/div/div[5]/div")
                except Exception as e:
                    break

                for comentario in comentarios:
                    try:
                        usuario = comentario.find_element(By.TAG_NAME, 'h3').text
                        comentario_texto = comentario.find_element(By.XPATH, './div/div').text

                        if comentario_texto != '':
                            data = [publicacion[5], usuario, comentario_texto]
                            comm.send(data, dest=3)
                            comm.send(comentario_texto, dest=1)
                            comm.send(usuario, dest=4)

                    except Exception as e:
                        try:
                            masComentarios = comentario.find_element(By.XPATH, './a')
                            if masComentarios.text == ' Ver más comentarios...':
                                time.sleep(3)
                                masComentarios.click()
                                aux = False
                            continue
                        except Exception as ex:
                            continue
                if aux:
                    break

        comm.send("Fin", dest=4)
        comm.send("Fin", dest=1)
        comm.send("Fin", dest=3)

        browser.quit()
        conn.commit()
        conn.close()
        print("Hola soy la pc: ", hostname, ". Ya termine de extraer los comentarios")

    if rank == 1:
        print("Hola soy la pc: ", hostname, ". Yo extraigo solo las palabras clave de cada comentario")
        const = True

        while const:
            data_recived = comm.recv(source=0)
            if data_recived != 'Fin':
                palabra = obtener_palabras_clave(data_recived)
                comm.send(palabra, dest=2)
            else:
                const = False
                comm.send('Fin', dest=2)
        print("Hola soy la pc: ", hostname, ". Ya termine de extraer solo las palabras clave de cada comentario")

    if rank == 2:
        print("Hola soy la pc: ", hostname, ". Yo cuento las veces que se repiten las palabras")
        const = True
        bolsa_palabras = Counter()

        while const:
            data_recived = comm.recv(source=1)

            if data_recived != 'Fin':
                bolsa_palabras.update(data_recived)
            else:
                with open('palabras.csv', 'a', newline='') as csvfile:
                    writer = csv.writer(csvfile)
                    for palabra, cantidad in bolsa_palabras.items():
                        writer.writerow([palabra, cantidad])
                const = False
        print("Hola soy la pc: ", hostname, ". Ya termine de contar las veces que se repiten las palabras")

    if rank == 3:
        print("Hola soy la pc: ", hostname, ". Yo guardo los comentarios en la base de datos")
        const = True

        while const:
            data_recived = comm.recv(source=0)
            if data_recived[0] != 'Fin':
                conn = sqlite3.connect('publicaciones.db')
                c = conn.cursor()
                c.execute("INSERT INTO comentarios VALUES (?, ?, ?)",
                          (data_recived[0], data_recived[1], data_recived[2]))
                conn.commit()
                conn.close()
            else:
                const = False
        print("Hola soy la pc: ", hostname, ". Ya termine de guardar los comentarios en la base de datos")

    if rank == 4:
        print("Hola soy la pc: ", hostname, ". Yo cuento el numero de comentarios de cada usuario")
        const = True
        bolsa_usuarios = Counter()

        while const:
            data_recived = comm.recv(source=0)

            if data_recived != 'Fin':
                bolsa_usuarios.update([data_recived])
            else:
                const = False
                with open('usuarios.csv', 'a', newline='') as csvfile:
                    writer = csv.writer(csvfile)
                    for usuario, cantidad in bolsa_usuarios.items():
                        writer.writerow([usuario, cantidad])
        print("Hola soy la pc: ", hostname, ". Ya termine de contar el numero de comentarios de cada usuario")
