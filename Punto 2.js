db.getCollection('Venta').aggregate(
[
    {
        "$group":{
            "_id": "$IdProducto",
            "Cantidad" : {
                "$sum" : 1
            }
        }
    },
    {
        "$sort":{
            "Cantidad": -1
        }
    }
])