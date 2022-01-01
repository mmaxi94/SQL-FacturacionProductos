import sqlite3
from sqlite3 import Error
import os
import csv

def leer_archivo(nombre_archivo):
    datos=[]
    with open(nombre_archivo,"r", encoding='utf-8') as f:
        lineas = f.readlines()
        count=0
        for linea in lineas:
            if count==0: #salteo el header del file
                count=count+1
            else:
                datos.append(tuple(linea.strip("\n ").split("|"))) #elimino fin de linea y espacios en blanco

    return datos

def sql_connection():
    try:
        con = sqlite3.connect('base_factprod.db')
        return con
    except Error:
        print(Error)

def sql_crear_tabla_cliente(con):
    cursorObj = con.cursor()
    cursorObj.execute("CREATE TABLE IF NOT EXISTS CLIENTE(id_cliente integer not null PRIMARY KEY, apellido text, nombre text)")
    con.commit()

def sql_crear_tabla_producto(con):
    cursorObj = con.cursor()
    cursorObj.execute("CREATE TABLE IF NOT EXISTS PRODUCTO(id_producto integer not null PRIMARY KEY, nombre_producto text, proveedor text,"
                      "categoria text, cantidad_x_unidad text, PrecioUnidad real, UnidadesEnExistencia integer)")
    con.commit()

def sql_crear_tabla_facturacion(con):
    cursorObj = con.cursor()
    cursorObj.execute("CREATE TABLE IF NOT EXISTS FACTURACION(id_factura text not null PRIMARY KEY, fecha date, cod_cliente integer,"
                      "cod_producto integer, cantidad integer, FOREIGN KEY(cod_cliente) REFERENCES CLIENTE(id_cliente), "
                      "FOREIGN KEY(cod_producto) REFERENCES PRODUCTO(id_producto))")
    con.commit()

def sql_insertar_cliente(con, data):
    cursorObj = con.cursor()
    for entities in data:
        try:
            cursorObj.execute('INSERT INTO CLIENTE VALUES(?, ?, ?)',
                              entities)
            con.commit()
        except sqlite3.IntegrityError as errFk:
            print(str(errFk) + " - registro: " + str(entities))
        except sqlite3.ProgrammingError as errPg:
            print(str(errPg) + " - registro: " + str(entities))


def sql_insertar_producto(con, data):
    cursorObj = con.cursor()
    for entities in data:
        try:
            cursorObj.execute('INSERT INTO PRODUCTO VALUES(?, ?, ?, ?, ?, ?, ?)',
                              entities)
            con.commit()
        except sqlite3.IntegrityError as errFk:
            print(str(errFk) + " - registro: " + str(entities))
        except sqlite3.ProgrammingError as errPg:
            print(str(errPg) + " - registro: " + str(entities))

def sql_insertar_facturacion(con, data):
    cursorObj = con.cursor()
    for entities in data:
        try:
            cursorObj.execute('INSERT INTO FACTURACION VALUES(?, ?, ?, ?, ?)',
                      entities)
            con.commit()
        except sqlite3.IntegrityError as errFk:
            print(str(errFk) + " - registro: " + str(entities))
        except sqlite3.ProgrammingError as errPg:
            print(str(errPg) + " - registro: " + str(entities))

def obtener_columnas_tablas(con, nombre_tabla):
    cursorObj = con.cursor()
    cursorObj.execute("PRAGMA table_info('CLIENTE')")
    rows = cursorObj.fetchall()
    field_list=[]
    for row in rows:
        field_list.append(row[1])

    print(tuple(field_list))

def listar_tabla(con, nombreTabla):
    cursorObj = con.cursor()
    obtener_columnas_tablas(con, nombreTabla)
    cursorObj.execute('SELECT * FROM '+ str(nombreTabla))
    rows = cursorObj.fetchall()
    for row in rows:
        print(row)

def guardar_ventas(con, nombre_archivo):
    cursorObj = con.cursor()
    cursorObj.execute("SELECT apellido||', '||nombre as nombre_cliente, nombre_producto, cantidad, PrecioUnidad, "
                                           "cantidad*PrecioUnidad as PrecioTotal FROM FACTURACION F "
    "INNER JOIN CLIENTE C ON C.ID_CLIENTE=F.cod_cliente INNER JOIN PRODUCTO P ON P.id_producto=F.cod_producto")
    rows = cursorObj.fetchall()
    header=['nombre_cliente','nombre_producto','cantidad','PrecioUnidad','PrecioTotal']

    with open(nombre_archivo, 'w', newline='') as csvfile:
        outputWriter = csv.writer(csvfile, delimiter='|')
        outputWriter.writerow(header)
        outputWriter.writerows(rows)

    print(tuple(header))
    for row in rows:
        print(row)


#Comienzo del programa principal
current_directory=os.getcwd()

nombre_archivo=current_directory+"/Cliente.txt"
ListaCliente=leer_archivo(nombre_archivo)

nombre_archivo=current_directory+"/Facturacion.txt"
ListaFacturacion=leer_archivo(nombre_archivo)

nombre_archivo=current_directory+"/Producto.txt"
ListaProducto=leer_archivo(nombre_archivo)

con = sql_connection()
cursorObj = con.cursor()
cursorObj.execute("PRAGMA foreign_keys = ON")

sql_crear_tabla_cliente(con)
sql_crear_tabla_producto(con)
sql_crear_tabla_facturacion(con)

print("\nInsercion de CLIENTES..")
sql_insertar_cliente(con,ListaCliente)
print("\nInsercion de PRODUCTOS..")
sql_insertar_producto(con, ListaProducto)
print("\nInsercion de FACTURACION..")
sql_insertar_facturacion(con, ListaFacturacion)

print("\nDatos TABLA CLIENTE: ")
listar_tabla(con,'CLIENTE')
print("\nDatos TABLA PRODUCTO: ")
listar_tabla(con,'PRODUCTO')
print("\nDatos TABLA FACTURACION: ")
listar_tabla(con,'FACTURACION')

nombre_archivo=current_directory+"/Ventas.txt"
print("\nCreacion archivo VENTAS..")
guardar_ventas(con,nombre_archivo)


con.close()