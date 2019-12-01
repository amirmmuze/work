
// counts mapping unknown dress codes by occasions
// relies on lookup and occasions_dress_codes_unwinded1 pre made collection

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
    {"$project": { "occasion":{ "$arrayElemAt": [ "$occasions.name", 0 ]},"gender":"$gender","parent_type":{ "$arrayElemAt": [ "$direct_parent.direct_parent", 0 ]},"parent_type_uk":{ "$arrayElemAt": [ "$direct_parent_uk.direct_parent", 0 ]},"product_type":"$product_type","dress_codes": {"$filter": {  "input": "$dress_codes", "as": "item",  "cond": { "$gte": [ "$$item.score", 100 ] }}}}},
    {"$match":{"dress_codes.0":{"$exists":false}}} ,
    {"$project":{"parent_type": { "$ifNull": [ "$parent_type", "$parent_type_uk" ] },"gender":"$gender","product_type":"$product_type","occasion":"$occasion"}},
    {"$project":{"parent_type":"$parent_type","product_type":"$product_type","gender":"$gender","occasion":"$occasion"}},
    {"$match": { "parent_type":{"$ne":null}} },
    {"$lookup":
            {
                "from": "occasions_dress_codes_unwinded1",
                "localField" : "occasion",
                "foreignField":"occasion",
                "as": "dress_code_mapping"
            }
    }  ,
    {'$match':{'dress_code_mapping.0':{"$exists":true}}},
    {"$project":{"occasion":"$occasion","parent_type":"$parent_type","product_type":"$product_type","gender":"$gender","dress_code":{ "$arrayElemAt": [ "$dress_code_mapping.dress_codes", 0 ]}}},
    {"$group":{"_id":{'dress_code':"$dress_code","gender":"$gender","parent_type":"$parent_type"},"count":{"$sum":1} }},
    {"$project":{"dress_code":"$_id.dress_code","gender":"$_id.gender","parent_type":"$_id.parent_type","count":"$count" ,"_id":0}},
    { "$sort" : { "count" : -1 }},
    {"$out" : "occasions_dress_codes_mapping_counts"}
]);
