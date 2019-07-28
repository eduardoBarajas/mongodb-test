db.getCollection('Venta').aggregate([
    {
        $lookup: {
            "from": "Cajeros",
            localField: "IdCajero",
            foreignField: "_id",
            as: "cajero"
        }
    },
    {
        $lookup: {
            from: "MaquinasRegistradoras",
            localField: "IdMaquina",
            foreignField: "_id",
            as: "maquina"
        }
    },
    {

        $lookup: {
            from: "Productos",
            localField: "IdProducto",
            foreignField: "_id",
            as: "producto"
        }

    },
    { $unwind: "$cajero" },
    { $unwind: "$producto" },
    { $unwind: "$maquina" },
    {
        $project: {
            PisoMaquina: "$maquina.Piso",
            IdProducto: "$producto.Producto",
            NombreProducto: "$producto.Nombre",
            PrecioProducto: "$producto.Precio",
            IdCajero: "$cajero.Cajero",
            NombreCajero: "$cajero.NomApels"
        }
    }
])