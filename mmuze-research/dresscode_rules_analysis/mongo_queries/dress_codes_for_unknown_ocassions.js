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
    {"$match":{"dress_codes.0":{"$exists":false},"occasions.0":{"$exists":true},"occasions.1":{"$exists":false},"occasions.score":100}} ,
    {"$project":{"parent_type": { "$ifNull": [ "$parent_type", "$parent_type_uk" ] },"gender":"$gender","product_type":"$product_type"}},
    {"$project":{"parent_type":"$parent_type","product_type":"$product_type","gender":"$gender"}},
    {"$match": { "parent_type":{"$ne":null}} },
    {"$lookup":
            {
                "from": "occasions_dress_codes_unwinded1",
                "localField" : "occasion",
                "foreignField":"dress_code",
                "as": "dress_code_mapping"
            }
    }  ,

    {"$project":{"parent_type":"$parent_type","product_type":"$product_type","gender":"$gender","dress_code":{ "$arrayElemAt": [ "$dress_code_mapping.dress_code", 0 ]}}},
    {"$group":{"_id":{'dress_code':"$dress_code","gender":"$gender","parent_type":"$parent_type"},"sample":{"$max":"$_id"},"count":{"$sum":1} }},
    {"$project":{"dress_code":"$_id.dress_code","gender":"$_id.gender","count":"$count" ,"_id":0,"sample":"$sample","parent_type":"$_id.parent_type"}},
    {"$group" : { "_id":{"gender":"$gender","count":"$count","parent_type":"$parent_type"},"keys" : { "$push" : "$dress_code" },"samples" : { "$push" : "$sample" },"sumValues" : { "$push" : "$count" },"total" : { "$sum" : "$count" }  }},
    { "$project" : {
            "dress_codes" : "$keys",
            "counts" : "$sumValues",
            "total" : "$total",
            "samples":"$samples",
            "percentages" : { "$map" : { "input" : "$sumValues", "as" : "s",
                    "in" : { "$divide" : ["$$s", "$total"] } } } }
    },


    {"$project":{"gender":"$_id.gender","parent_type":"$_id.parent_type","dress_codes":"$dress_codes","percentages":"$percentages" ,"total_count":"$total","_id":0 }},
    {"$project":{"gender":"$gender","parent_type":"$parent_type","dress_code":{ "$arrayElemAt": [ "$dress_codes", 0 ]},"total_count":"$total_count","_id":0   }},
    {"$out" : "occasions_dress_codes_mapping_2"}
]);
