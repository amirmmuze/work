db.getCollection('occasions').aggregate([{"$unwind":"$dress_codes"},
    {"$project":{"_id":0,"dress_codes":1,"occasion":1}},{"$out":"occasions_unwinded"}])