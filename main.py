import sqlite3
import extraer_publicaciones

if __name__ == '__main__':

    # conn = sqlite3.connect('../practica-05/publicaciones.db')
    # c = conn.cursor()
    # c.execute('''CREATE TABLE IF NOT EXISTS publicaciones(termino TEXT, fecha DATE, usuario TEXT, cuerpo TEXT, reacciones INTEGER, url TEXT)''')
    # conn.commit()
    # conn.close()

    #terminos = ["EleccionesAnticipadas2023", "YaTenemosPresidenta", "FernandoVillavicencio"]
    terminos = ["CNE2023", "ELecciones2023", "RafaelCorrea", "JorgeGlass", "AlvaroNoboa2023", "Elecciones2023"]
    cantidadPublicaciones = 0

    for termino in terminos:
        cantidadPublicaciones = cantidadPublicaciones + extraer_publicaciones.extaerBy(termino)

    print("Numero de publicaciones procesadas:", str(cantidadPublicaciones))

