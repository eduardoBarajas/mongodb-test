db.getCollection('Venta').aggregate([
    {
        "$lookup": {
            from: "Cajeros",
            localField: "IdCajero",
            foreignField: "_id",
            as: "cajero"
        }
    },
    {
        "$lookup": {
            from: "Productos",
            localField: "IdProducto",
            foreignField: "_id",
            as: "producto"
        }
    },
    { "$unwind": "$cajero" },
    { "$unwind": "$producto" },
    {
        "$group": {
            "_id": "$cajero._id",
            "Importe": { "$sum": "$producto.Precio" },
            "Cajero": { $addToSet: "$cajero" }
        }
    }, {
        "$sort": {
            "Importe": -1
        }
    },
    { "$unwind": "$Cajero" },
    // con project se define cual sera la respuesta.
    {
        "$project": {
            "idCajero": "$cajero._id",
            "NombreCajero": "$Cajero.NomApels",
            "Importe": "$Importe"
        }
    }
])
