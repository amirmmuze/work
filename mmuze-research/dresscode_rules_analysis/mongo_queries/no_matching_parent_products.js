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
        {
            "$lookup":
                {
                    "from": "product_types_uk",
                    "localField": "product_type",
                    "foreignField": "name",
                    "as": "direct_parent_uk"
                }
        },
        {"$project": { "gender":"$gender","parent_type":{ "$arrayElemAt": [ "$direct_parent.direct_parent", 0 ]},"parent_type_uk":{ "$arrayElemAt": [ "$direct_parent_uk.direct_parent", 0 ]},"product_type":"$product_type","dress_codes": {"$filter": {  "input": "$dress_codes", "as": "item",  "cond": { "$gte": [ "$$item.score", 100 ] }}}}},
        {"$match":{"dress_codes.0":{"$exists":true},"dress_codes.1":{"$exists":false},"dress_codes.0.score":100,"parent_type":null,"parent_type_uk":null}} ,
        {"$group":{"_id":{'dress_codes':"$dress_codes.name","gender":"$gender","parent_type":"$parent_type","product_type":"$product_type"},"count":{"$sum":1} }},
        {"$project":{"dress_code":{ "$arrayElemAt": [ "$_id.dress_codes", 0 ] },"gender":"$_id.gender","parent_type":"$_id.parent_type","product_type":"$_id.product_type","count":"$count" ,"_id":0}},
        { "$sort" : { "count" : -1 }},
        {"$out" : "no_matching_parent_type"}




    ],
    {
        "allowDiskUse":true
    }
);