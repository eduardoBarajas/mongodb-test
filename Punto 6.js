db.getCollection('Venta').aggregate([
    {
        // se obtiene el cajero usando el IdCajero del documento venta
        "$lookup": {
            from: "Cajeros",
            localField: "IdCajero",
            foreignField: "_id",
            as: "cajero"
        }
    },
    {
        // se obtiene la maquina usando el IdMaquina del documento venta
        "$lookup": {
            from: "MaquinasRegistradoras",
            localField: "IdMaquina",
            foreignField: "_id",
            as: "maquina"
        }
    },
    {
        // se obtiene el producto usando el IdProducto del documento venta
        "$lookup": {
            from: "Productos",
            localField: "IdProducto",
            foreignField: "_id",
            as: "producto"
        }
    },
    // se utiliza $unwind para descontruir el arreglo obtenido en un objecto.
    { "$unwind": "$cajero" },
    { "$unwind": "$producto" },
    { "$unwind": "$maquina" },
    {
        // se obtiene todos los registros de los pisos, se realiza la sumatoria del importe y se almacena el set de los cajeros.
        "$group": {
            "_id": "$maquina.Piso",
            "Importe": { "$sum": "$producto.Precio" },
            "Cajeros": { $addToSet: "$cajero" }
        }
    }
    , {
        "$sort": {
            "Importe": -1
        }
    },
    // con project se define cual sera la respuesta, si el importe es menor a 500 se regresa el documento, de lo contrario se enviara un null.
    {
        $project: {
            result: {
                $cond: { if: { $lte: ["$Importe", 500] }, then: { Importe: "$Importe", Piso: "$maquina.Piso", Cajeros: "$Cajeros" }, else: null }
            }
        }
    }
])
