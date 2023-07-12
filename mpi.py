import sys
import math
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


def comentarios(publicaciones, i):
    url = "https://mbasic.facebook.com"
    browser = webdriver.Chrome()
    browser.get(url)
    browser.find_element(By.CSS_SELECTOR, '[name="email"]').send_keys("leonelmessi23311@gmail.com")
    browser.find_element(By.CSS_SELECTOR, '[name="pass"]').send_keys("123987@1")
    time.sleep(i)
    browser.find_element(By.CSS_SELECTOR, '[name="login"]').click()

    bolsa_palabras = Counter()
    bolsa_usuarios = Counter()

    for publicacion in publicaciones:
        browser.get(publicacion[5])
        time.sleep(1)

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
                        c.execute("INSERT INTO comentarios VALUES (?, ?, ?)",
                                  (publicacion[5], usuario, comentario_texto))

                        # Actualizar la bolsa de palabras
                        palabras_clave = obtener_palabras_clave(comentario_texto)
                        bolsa_palabras.update(palabras_clave)
                    bolsa_usuarios.update([usuario])
                except Exception as e:
                    try:
                        masComentarios = comentario.find_element(By.XPATH, './a')
                        if masComentarios.text == ' Ver más comentarios…':
                            masComentarios.click()
                            time.sleep(1)
                            aux = False
                        continue
                    except Exception as ex:
                        continue
            if aux:
                break

    browser.quit()
    conn.commit()
    conn.close()
    print("Termino de extraer los comentarios")
    bolsa_palabras_ordenada = sorted(bolsa_palabras.most_common(), key=lambda x: x[1], reverse=True)
    bolsa_usuarios_ordenada = sorted(bolsa_usuarios.most_common(), key=lambda x: x[1], reverse=True)

    return bolsa_palabras_ordenada, bolsa_usuarios_ordenada


if __name__ == '__main__':
    # Configurar la codificación de la salida estándar

    sys.stdout.reconfigure(encoding='utf-8')

    conn = sqlite3.connect('publicaciones.db')
    c = conn.cursor()
    # c.execute('''CREATE TABLE IF NOT EXISTS comentarios(publicacion TEXT,usuario TEXT,comentario TEXT)''')
    c.execute("SELECT * FROM publicaciones LIMIT 10")
    publicaciones = c.fetchall()
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()
    hostname = MPI.Get_processor_name()

    salto = math.ceil(len(publicaciones) / (size - 1))  # División redondeada hacia arriba

    if rank == 0:
        # Proceso principal
        results = []
        for i in range(1, size):
            inicio = (i - 1) * salto
            fin = min(inicio + salto, len(publicaciones))
            comm.send(publicaciones[inicio:fin], dest=i, tag=1)

        for i in range(1, size):
            result = comm.recv(source=i, tag=2)
            print("Recibido de proceso", i, ":", result)
            results.extend(result)


        # Guardar bolsa de palabras en un archivo CSV
        with open('bolsa_palabras.csv', 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(['Palabra', 'Frecuencia'])
            writer.writerows(results[0])

        # Guardar bolsa de usuarios en un archivo CSV
        with open('bolsa_usuarios.csv', 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(['Usuario', 'Frecuencia'])
            writer.writerows(results[1])

    else:
        # Procesos secundarios
        print("Hola mi rank es: ", rank, "y mi hostname es: ", hostname)
        publicaciones_part = comm.recv(source=0, tag=1)
        resultado = comentarios(publicaciones_part, rank * 3)
        comm.send(resultado, dest=0, tag=2)