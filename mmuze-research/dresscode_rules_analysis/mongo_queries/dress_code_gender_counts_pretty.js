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
        {"$match":{"dress_codes.0":{"$exists":true},"dress_codes.1":{"$exists":false},"dress_codes.0.score":100}} ,
        {"$group":{"_id":{'dress_codes':"$dress_codes.name","gender":"$gender","parent_type":"$parent_type"},"count":{"$sum":1} }},
        {"$project":{"dress_code":{ "$arrayElemAt": [ "$_id.dress_codes", 0 ] },"gender":"$_id.gender","parent_type":"$_id.parent_type","count":"$count" ,"_id":0}},
        { "$sort" : { "count" : -1 }}
    ],
    {
        "allowDiskUse":true
    }
).toArray()