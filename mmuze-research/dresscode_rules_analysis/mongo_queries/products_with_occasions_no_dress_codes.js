db.getCollection('retailer_products').aggregate([
    {"$match":{"subverticals":"clothing","occasions.0":{"$exists":true},"occasions.1":{"$exists":false},"occasions.score":100}},
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
    {"$project": { "gender":"$gender","occasion":{ "$arrayElemAt": [ "$occasions.name", 0 ]},"parent_type":{ "$arrayElemAt": [ "$direct_parent.direct_parent", 0 ]},"parent_type_uk":{ "$arrayElemAt": [ "$direct_parent_uk.direct_parent", 0 ]},"product_type":"$product_type","dress_codes": {"$filter": {  "input": "$dress_codes", "as": "item",  "cond": { "$gte": [ "$$item.score", 100 ] }}}}},
    {"$match":{"dress_codes.0":{"$exists":false}}} ,
    {"$project":{"occasion":"$occasion","parent_type": { "$ifNull": [ "$parent_type", "$parent_type_uk" ] },"gender":"$gender","product_type":"$product_type"}},
    {"$match": { "parent_type":{"$ne":null}} },
    {"$lookup":
            {
                "from": "occasions_dress_codes_unwinded1",
                "localField" : "occasion",
                "foreignField":"occasion",
                "as": "dress_code_mapping"
            }
    }  ,

    {"$project":{"occasion":"$occasion","parent_type":"$parent_type","product_type":"$product_type","gender":"$gender","dress_code":{ "$arrayElemAt": [ "$dress_code_mapping.dress_codes", 0 ]}}},
    {"$match": { "dress_code":{"$ne":null}} },
    {"$out" : "occasions_dress_codes_mapping_products"}
]);
