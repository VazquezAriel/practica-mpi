from selenium import webdriver
from selenium.webdriver.common.by import By
from datetime import datetime, timedelta
import time
import sqlite3

meses = {
    "enero": "01",
    "febrero": "02",
    "marzo": "03",
    "abril": "04",
    "mayo": "05",
    "junio": "06",
    "julio": "07",
    "agosto": "08",
    "septiembre": "09",
    "octubre": "10",
    "noviembre": "11",
    "diciembre": "12"
}

def agregar(termino, fecha, usuario, texto, reaccion, url):

    # Conectarse a la base de datos
    conn = sqlite3.connect('../practica-05/publicaciones.db')
    c = conn.cursor()

    # Verificar si la URL ya existe en la base de datos
    url_existente = c.execute("SELECT COUNT(*) FROM publicaciones WHERE url = ?", (url,)).fetchone()[0]

    # Agregar el registro solo si la URL no existe
    if url_existente == 0:
        c.execute("INSERT INTO publicaciones VALUES (?, ?, ?, ?, ?, ?)", (termino, fecha, usuario, texto, reaccion, url))
        conn.commit()
    else:
        print("La URL ya existe en la base de datos. No se agregó el registro.")

    # Cerrar la conexión
    conn.close()

def extraerFecha(fecha_str):

    if "h" in fecha_str:
        fecha = datetime.now()
    elif "min" in fecha_str:
        fecha = datetime.now()
    elif "de" in fecha_str:
        fecha_str = fecha_str.split(" a ")[0] + " de 2023"
        for mes, numero in meses.items():
            if mes in fecha_str.lower():
                fecha_str = fecha_str.replace(mes, numero)
                break
        formato = "%d de %m de %Y"
        fecha = datetime.strptime(fecha_str, formato)
    elif "Ayer" in fecha_str:
        fecha = datetime.now() - timedelta(days=1)
    else:
        fecha = datetime.now()
    return fecha

def extraerTexto(parrafo):
    texto = ''
    for frase in parrafo:
        texto += frase.text
    return texto

def extaerBy(termino):

    contador = 0

    url = "https://mbasic.facebook.com"
    url_busqueda = "https://mbasic.facebook.com/search/posts/?q="
    url_hashtag = "https://mbasic.facebook.com/hashtag/"

    browser = webdriver.Chrome()
    browser.get(url)
    browser.find_element(By.CSS_SELECTOR, '[name="email"]').send_keys("leonelmessi23311@gmail.com")
    browser.find_element(By.CSS_SELECTOR, '[name="pass"]').send_keys("123987@1")
    browser.find_element(By.CSS_SELECTOR, '[name="login"]').click()
    browser.get(url_hashtag + termino)

    while (True):

        # Publicacion
        publicaciones = browser.find_elements(By.TAG_NAME, "article")

        for publicacion in publicaciones:

            try:
                # Fecha de la publicacion
                fecha = extraerFecha(publicacion.find_element(By.TAG_NAME, "abbr").text)

                # Usuario de la publicacion
                usuario = publicacion.find_element(By.TAG_NAME, "h3").text

                # Texto de la publicacion
                texto = extraerTexto(publicacion.find_elements(By.TAG_NAME, "p"))

                # Reacciones de la publicacion
                reacciones = int(publicacion.find_element(By.XPATH, "./footer/div[2]/span/a").text.replace('.', ''))

                # Reacciones de la publicacion
                url = publicacion.find_element(By.XPATH, "./footer/div[2]/a[3]").get_attribute("href")

                # Agregar a la BD
                agregar(termino, fecha, usuario, texto, reacciones, url)
                contador = contador+1

            except Exception as e:
                continue
        try:
            element = browser.find_element(By.ID, 'see_more_pager')
            element.find_element(By.TAG_NAME, 'a').click()
            time.sleep(1)

        except Exception as e:
            break

    browser.quit()
    print("Termino de extraer los datos")
    return contador
