#!/usr/bin/env python3
import pymongo
from pymongo import MongoClient
import random
import pprint
import os
"""
CONSTANTES
"""
MY_COLLECTIONS = ["Cajeros", "MaquinasRegistradoras", "Venta", "Productos"]
MY_COLLECTIONS_IDS = {"IdCajero": "Cajeros", "IdMaquina": "MaquinasRegistradoras", "IdVenta":"Venta", "IdProducto":"Productos"}
MY_COLLECTIONS_FIELDS = {"Cajeros": [{"Campo":"_id", "Tipo":"int"}, {"Campo":"NomApels", "Tipo":"string"}], "MaquinasRegistradoras":
        [{"Campo":"_id", "Tipo":"int"}, {"Campo":"Piso", "Tipo":"int"}], "Venta": [{"Campo":"IdCajero", "Tipo":"int"}, {"Campo":"IdMaquina", "Tipo":"int"},
         {"Campo":"IdProducto", "Tipo":"int"}], "Productos": [{"Campo":"_id", "Tipo":"int"}, {"Campo":"Nombre", "Tipo":"string"}, {"Campo":"Precio", "Tipo":"float"}]}
DB_NAME = "Almacenes"
DB_CONNECTION_STRING = "mongodb://localhost:27017/"
def getDatabase(client, db_name):
    if (checkIfDatabaseExists(client, db_name)):
        print("DB Obtenida con exito")
    else: 
        print("DB Creada")
    return client[db_name]

def checkIfDatabaseExists(client, db_name):
    if (db_name in client.list_database_names()):
        return True
    else:
        return False

def getDocumentById(options, id, database):
    # en options se reciben los parametros que se utilizaran para la busqueda
    # en id se recibe el tipo de id para obtener la coleccion ej. MY_COLLECTIONS[IdProducto] obtendra la coleccion de productos. 
    return database[MY_COLLECTIONS_IDS[id]].find(options).count()

def addNewEntry(collection_name, database):
    os.system("clear")
    print("Agregando Nuevo Registro En: {}".format(collection_name))
    query = {}
    try: 
        # Se obtiene los campos de la coleccion para agregar los datos del documento.
        for field in MY_COLLECTIONS_FIELDS[collection_name]:
            if (collection_name == "Venta"):
                new_val = int(input("\tTipo: {}\n\tIngresa el {} :".format(field["Tipo"], field["Campo"])))
                # su el documento no existe con ese id se regresa un error y se cancela la adicion, de lo contrario se agrega.
                if (getDocumentById({"_id": new_val}, field["Campo"], database) == 0):
                    query = {}
                    raise ValueError("No existe un documento: {} con ese id".format(field["Campo"]))
                else:
                    query[field["Campo"]] = new_val
            # si no es la coleccion de ventas entonces se agrega el dato dependiendo el tipo de dato especificado en el diccionario.
            elif (field["Tipo"] == "int"):
                query[field["Campo"]] = int(input("\tTipo: {}\n\tIngresa el {} :".format(field["Tipo"], field["Campo"])))
            elif (field["Tipo"] == "string"):
                query[field["Campo"]] = str(input("\tTipo: {}\n\tIngresa el {} :".format(field["Tipo"], field["Campo"])))
            elif (field["Tipo"] == "float"):
                query[field["Campo"]] = float(input("\tTipo: {}\n\tIngresa el {} :".format(field["Tipo"], field["Campo"])))
    except ValueError as Error: 
        os.system("clear")
        print(Error)
    print(query)
    # si la longitud del diccionario de query es mayor a 0 se intenta agregar el documento
    if (len(query) > 0):
        try:
            result = database[collection_name].insert_one(query).inserted_id
            if (result):
                print("Se agrego con exito.")
            else: 
                print("No se agrego")
        except pymongo.errors.DuplicateKeyError as Error:
            print(Error)
    else:
        print("No se agrego")
    input('Presiona enter para regresar')

if (__name__ == "__main__"):
    running = True
    client = MongoClient(DB_CONNECTION_STRING)
    db = getDatabase(client, DB_NAME)
    while (running):
        os.system("clear")
        print("Utilizando la base de datos: {}".format(DB_NAME))
        print("\n1) Agregar registro")
        print("2) Ejecutar query 1")
        print("3) Ejecutar query 2")
        print("4) Ejecutar query 3")
        print("5) Ejecutar query 4")
        print("6) Ejecutar query 5")
        print("7) Generar 200 ventas")
        print("0 para salir\n")
        opt = int(input("Elige una opcion: "))
        if (opt == 0):
            running = False
        elif (opt == 1):
            adding = True
            while (adding):
                os.system("clear")
                print("Agregando Nuevo Registro")
                print("\t1) Cajero")
                print("\t2) Maquina Registradora")
                print("\t3) Venta")
                print("\t4) Producto")
                print("\t0) Regresar")
                opt2 = int(input("Elige una opcion: "))
                if (opt2 == 0):
                    adding = False
                elif (opt2 == 1):
                    addNewEntry("Cajeros", db)
                elif (opt2 == 2):
                    addNewEntry("MaquinasRegistradoras", db)
                elif (opt2 == 3):
                    addNewEntry("Venta", db)
                elif (opt2 == 4):
                    addNewEntry("Productos", db)
        elif (opt == 2):
            print("Mostrar el número de ventas de cada producto, ordenado de más a menos ventas")
            for document in db["Venta"].aggregate([{"$group":{"_id": "$IdProducto", "Cantidad" : {"$sum" : 1}}},{"$sort":{"Cantidad": -1}}]):
                pprint.pprint(document)
            input("Presiona Enter Para Continuar")
        elif (opt == 3):
            print("Obtener un informe completo de ventas, indicando el nombre del cajero que realizo la venta, nombre y precios de los productos vendidos, y el piso en el que se encuentra la máquina registradora donde se realizó la venta.")
            for document in db["Venta"].aggregate([{"$lookup": { "from": "Cajeros", "localField": "IdCajero", "foreignField": "_id", "as": "cajero" }},{ "$lookup": { "from": "MaquinasRegistradoras", "localField": "IdMaquina", "foreignField": "_id", "as": "maquina" }},{ "$lookup": { 
                "from": "Productos", "localField": "IdProducto", "foreignField": "_id", "as": "producto" }}, { "$unwind":"$cajero" }, { "$unwind":"$producto" }, { "$unwind":"$maquina" }, {"$project": {
                "PisoMaquina" : "$maquina.Piso", "IdProducto" : "$producto.Producto", "NombreProducto" : "$producto.Nombre", "PrecioProducto": "$producto.Precio", "IdCajero": "$cajero.Cajero", "NombreCajero": "$cajero.NomApels"}} ]):
                pprint.pprint(document)   
            input("Presiona Enter Para Continuar")
        elif (opt == 4):
            print("Obtener las ventas totales realizadas en cada piso")
            for document in db["Venta"].aggregate([{"$lookup": { "from": "MaquinasRegistradoras", "localField": "IdMaquina", "foreignField": "_id", "as": "maquina" }}, { "$unwind":"$maquina" },{"$group":{"_id": "$maquina.Piso","Cantidad" : {"$sum" : 1}
                }},{"$sort":{"Cantidad": -1}}]):
                pprint.pprint(document)   
            input("Presiona Enter Para Continuar")
        elif (opt == 5):
            print("Obtener el código y nombre de cada cajero junto con el importe total de sus ventas")
            for document in db["Venta"].aggregate([{"$lookup": { "from": "Cajeros", "localField": "IdCajero", "foreignField": "_id", "as": "cajero" }},{"$lookup": { "from": "Productos","localField": "IdProducto","foreignField": "_id","as": "producto" }},
                { "$unwind":"$cajero" },{ "$unwind":"$producto" },{"$group":{"_id": "$cajero._id","Importe" : {"$sum" : "$producto.Precio"},"Cajero": {"$addToSet" : "$cajero"}}},{"$sort":{"Importe": -1}},{ "$unwind":"$Cajero" },{"$project": {"idCajero": "$cajero._id",
                "NombreCajero": "$Cajero.NomApels","Importe": "$Importe"}}]):
                pprint.pprint(document)   
            input("Presiona Enter Para Continuar")
        elif (opt == 6):
            print("Obtener el código y nombre de aquellos cajeros que hayan realizado ventas en pisos cuyas ventas totales sean inferiores a los 500 pesos")
            for document in db["Venta"].aggregate([{"$lookup": { "from": "Cajeros","localField": "IdCajero","foreignField": "_id","as": "cajero" }},{"$lookup": { "from": "MaquinasRegistradoras","localField": "IdMaquina","foreignField": "_id","as": "maquina" }},{"$lookup": { 
                "from": "Productos","localField": "IdProducto","foreignField": "_id","as": "producto" }},{ "$unwind":"$cajero" },{ "$unwind":"$producto" },{ "$unwind":"$maquina" },{"$group":{"_id": "$maquina.Piso","Importe" : {"$sum" : "$producto.Precio" },"Cajeros": {"$addToSet" : "$cajero"}
                }},{"$sort":{"Importe": -1}},{"$project": {"result": {"$cond": { "if": { "$lte": [ "$Importe", 500 ] }, "then": {"Importe" : "$Importe", "Piso" : "$maquina.Piso","Cajeros": "$Cajeros"}, "else": "null" }}}} ]):
                pprint.pprint(document)   
            input("Presiona Enter Para Continuar")
        elif (opt == 7):
            for n in range(200):
                print(db["Venta"].insert_one({"IdCajero": random.randint(1, 10), "IdMaquina": random.randint(1, 16), "IdProducto": random.randint(1, 12)}).inserted_id)