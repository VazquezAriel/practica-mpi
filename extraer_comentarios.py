from selenium import webdriver
from selenium.webdriver.common.by import By
import time
import sqlite3
from mpi4py import MPI

if __name__ == '__main__':

    comm = MPI.COMM_WORLD
    rank = comm.rank
    hostname = MPI.Get_processor_name()

    if rank == 0:

        conn = sqlite3.connect('../practica-05/publicaciones.db')
        c = conn.cursor()
        c.execute("SELECT * FROM publicaciones LIMIT 5")
        publicaciones = c.fetchall()

        url = "https://mbasic.facebook.com"

        browser = webdriver.Chrome()
        browser.get(url)
        browser.find_element(By.CSS_SELECTOR, '[name="email"]').send_keys("leonelmessi23311@gmail.com")
        browser.find_element(By.CSS_SELECTOR, '[name="pass"]').send_keys("123987@1")
        browser.find_element(By.CSS_SELECTOR, '[name="login"]').click()

        for publicacion in publicaciones:

            browser.get(publicacion[5])
            print("My rank es: ", rank, "  mi hostname es: ", hostname, " y la url es:", publicacion[5])

            time.sleep(1)

            while (True):
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
                        comentario = comentario.find_element(By.XPATH, './div/div').text

                        if comentario != '':
                            c.execute("INSERT INTO comentarios VALUES (?, ?, ?)", (publicacion[5], usuario, comentario))

                    #     Aqui hacemos la bolsa de palabras:

                    #     Aqui hacemos la bolsa de usuarios:

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


    if rank == 1:

        conn = sqlite3.connect('../practica-05/publicaciones.db')
        c = conn.cursor()
        c.execute("SELECT * FROM publicaciones LIMIT 5 OFFSET 5")
        publicaciones = c.fetchall()

        url = "https://mbasic.facebook.com"

        browser = webdriver.Chrome()
        browser.get(url)
        browser.find_element(By.CSS_SELECTOR, '[name="email"]').send_keys("leonelmessi23311@gmail.com")
        browser.find_element(By.CSS_SELECTOR, '[name="pass"]').send_keys("123987@1")
        browser.find_element(By.CSS_SELECTOR, '[name="login"]').click()

        for publicacion in publicaciones:

            browser.get(publicacion[5])

            print("My rank es: ", rank, "  mi hostname es: ", hostname, " y la url es:", publicacion[5])

            time.sleep(1)

            while (True):
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
                        comentario = comentario.find_element(By.XPATH, './div/div').text

                        if comentario != '':
                            c.execute("INSERT INTO comentarios VALUES (?, ?, ?)", (publicacion[5], usuario, comentario))

                    #     Aqui hacemos la bolsa de palabras:

                    #     Aqui hacemos la bolsa de usuarios:

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




