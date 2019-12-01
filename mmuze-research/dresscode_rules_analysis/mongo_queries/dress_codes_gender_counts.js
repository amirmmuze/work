db.getCollection('retailer_products').aggregate([
        {"$match":{"subverticals":"clothing"}},
        {
            "$lookup":
                {
                    "from": "product_types",
                    "localField": "product_type",
                    "foreignField": "name",
                    "as": "direct_parent"
                }
        },
        {"$project": { "gender":"$gender","parent_type":{ "$arrayElemAt": [ "$direct_parent.direct_parent", 0 ]},"dress_codes": {"$filter": {  "input": "$dress_codes", "as": "item",  "cond": { "$gte": [ "$$item.score", 100 ] }}}}},
        {"$match":{"dress_codes.0":{"$exists":true}}} ,
        {"$unwind": "$dress_codes"},
        {"$sort": {'dress_codes.name': 1}},
        {"$group": {"_id": {'id':"$_id","gender":"$gender","parent_type":"$parent_type"}, 'dress_codes': {"$push": '$dress_codes'}}},
        {"$group":{"_id":{'dress_codes':"$dress_codes.name","gender":"$_id.gender","parent_type":"$_id.parent_type"},"count":{"$sum":1} }},
        { "$sort" : { "count" : -1 }}
    ],
    {
        "allowDiskUse":true
    }
).toArray()
