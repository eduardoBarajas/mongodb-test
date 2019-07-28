db.getCollection('Venta').aggregate([
    {
        "$lookup": {
            from: "MaquinasRegistradoras",
            localField: "IdMaquina",
            foreignField: "_id",
            as: "maquina"
        }
    },
    { "$unwind": "$maquina" },
    {
        "$group": {
            "_id": "$maquina.Piso",
            "Cantidad": { "$sum": 1 }
        }
    }, {
        "$sort": {
            "Cantidad": -1
        }
    }
])
