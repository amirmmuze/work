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
    {"$match":{"dress_codes.0":{"$exists":true},"dress_codes.1":{"$exists":false},"dress_codes.0.score":100}} ,
    {"$project":{"parent_type": { "$ifNull": [ "$parent_type", "$parent_type_uk" ] },"gender":"$gender",'dress_codes_names':'$dress_codes.name',"product_type":"$product_type"}},
    {"$project":{"parent_type":"$parent_type","product_type":"$product_type","gender":"$gender","dress_code":{ "$arrayElemAt": [ "$dress_codes_names", 0 ]}}},
    {"$match": { "parent_type":{"$ne":null}} },

    {"$lookup":
            {
                "from": "dress_code_tags_unwinded",
                "localField" : "dress_code",
                "foreignField":"synonyms",
                "as": "dress_code_main_synonym"
            }
    }  ,

    {"$project":{"parent_type":"$parent_type","product_type":"$product_type","gender":"$gender","dress_code":{ "$arrayElemAt": [ "$dress_code_main_synonym.dress_code", 0 ]}}},

    {"$lookup":
            {
                "from": "occasions",
                "localField" : "dress_code",
                "foreignField":"dress_codes",
                "as": "occasion_for_dress_code"
            }
    },
    {"$group":{"_id":{'dress_code':"$dress_code","gender":"$gender","parent_type":"$parent_type","occasion":{ "$arrayElemAt": [ "$occasion_for_dress_code.occasion", 0 ]}},"sample":{"$max":"$_id"},"count":{"$sum":1} }},
    {"$project":{"dress_code":"$_id.dress_code","gender":"$_id.gender","count":"$count" ,"_id":0,"sample":"$sample","occasion":"$_id.occasion","parent_type":"$_id.parent_type"}},
    {"$group" : { "_id":{"gender":"$gender","parent_type":"$parent_type","occasion":"$occasion"},"keys" : { "$push" : "$dress_code" },"samples" : { "$push" : "$sample" },"sumValues" : { "$push" : "$count" },"total" : { "$sum" : "$count" }  }},
    { "$project" : {
            "dress_codes" : "$keys",
            "counts" : "$sumValues",
            "total" : "$total",
            "samples":"$samples",
            "percentages" : { "$map" : { "input" : "$sumValues", "as" : "s",
                    "in" : { "$divide" : ["$$s", "$total"] } } } }
    },

    {"$project":{"gender":"$_id.gender","parent_type":"$_id.parent_type","occasion":"$_id.occasion","dress_codes":"$dress_codes","percentages":"$percentages","counts":"$counts" ,"total_count":"$total","_id":0,   "samples":"$samples",
        }},

    {"$out" : "occasions_dresscodes"}
]);
