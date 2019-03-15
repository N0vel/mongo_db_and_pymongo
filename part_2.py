import pymongo
import json
from pprint import pprint


client = pymongo.MongoClient('mongodb://localhost:27017/')
database = client.my_database
database.shakespeare.drop()

with open('shakespeare_plays.json') as in_file:
    data = [json.loads(line) for line in in_file]

shakespeare = database.shakespeare
shakespeare.insert_many(data)
print(shakespeare.count_documents({})) 

pipeline = [
            {'$unwind': "$acts"},
            {'$unwind': "$acts.scenes"},
            {'$unwind': "$acts.scenes.action"},
            {"$project": 
                {"lines": {'$size': "$acts.scenes.action.says"},
                "play": "$_id",
                "act": "$acts.title",
                "scene": "$acts.scenes.title"
                }
            },
            {"$group": 
                {"_id": 
                    {"play": "$play",
                     "act": "$act",
                      "scene": "$scene"
                    },
                "lines": 
                    {"$sum": "$lines"
                    }
                }
            },
            {"$sort": {"lines": -1}},
            {"$limit": 1}
            ]

pprint(list(shakespeare.aggregate(pipeline)))
pipeline = [
            {"$unwind": '$acts'},
            {"$project": 
                {
                "play": "$_id",
                "acts": {'$add': 1},
                "scenes": {'$size': "$acts.scenes"}
                }
            },
            {"$group": 
                {"_id": "$play",
                "acts": {"$sum": "$acts"},
                "scenes": {"$sum": "$scenes"}
                }
            }
            ]
pprint(list(shakespeare.aggregate(pipeline)))
pipeline = [
            {"$unwind": '$acts'},
            {"$unwind": '$acts.scenes'},
            {"$unwind": '$acts.scenes.action'},
            {"$group": 
                {"_id": "$_id",
                "characters": {'$addToSet': '$acts.scenes.action.character'}
                }
            },
            {"$unwind": "$characters"},
            {"$sort": {"characters": 1}},
            {"$group":
                {"_id": "$_id",
                "characters": {"$push": "$characters"}
                }
            }
            ]
pprint(list(shakespeare.aggregate(pipeline)))

pipeline = [
            {'$unwind': "$acts"},
            {'$unwind': "$acts.scenes"},
            {'$unwind': "$acts.scenes.action"},
            {"$project": 
                {"lines": {'$add': [1]},
                "character": "$acts.scenes.action.character"}
            },
            {"$group":
                {"_id": "$character",
                "lines": {"$sum": "$lines"}
                }
            },
            {"$match": {"_id": "JULIET"}}
            ]
pprint(list(shakespeare.aggregate(pipeline)))
pipeline = [
            {'$unwind': "$acts"},
            {'$unwind': "$acts.scenes"},
            {'$unwind': "$acts.scenes.action"},
            {"$project": 
                {"play": "$_id",
                "character": "$acts.scenes.action.character"}
            },
            {"$unwind": "$character"},
            {"$group":
                {"_id": "$character",
                "plays": {"$addToSet": "$play"}
                }
            },
            {"$project":
                {"character": "$_id",
                "plays": "$plays",
                "count": {"$size": "$plays"}
                }
            },
            {"$match": {'count': {"$gt": 1}}}
            ]
pprint(list(shakespeare.aggregate(pipeline)))
pipeline = [
            {'$unwind': "$acts"},
            {'$unwind': "$acts.scenes"},
            {'$unwind': "$acts.scenes.action"},
            {"$project": 
                {"play": "$_id",
                "character": "$acts.scenes.action.character"}
            },
            {"$unwind": "$character"},
            {"$group":
                {"_id": "$character",
                "plays": {"$addToSet": "$play"}
                }
            },
            {"$project":
                {"character": "$_id",
                "plays": "$plays",
                "count": {"$size": "$plays"}
                }
            },
            {"$match": {'count': {"$gt": 1}}}
            ]
pprint(list(shakespeare.aggregate(pipeline)))
pipeline = [
            {'$unwind': "$acts"},
            {'$unwind': "$acts.scenes"},
            {'$unwind': "$acts.scenes.action"},
            {"$project": 
                {"play": "$_id",
                "character": "$acts.scenes.action.character",
                "lines": {'$add': [1]}
                }
            },
            {"$group": 
                {"_id": 
                    {"play": "$play",
                    "character": "$character"
                    },
                "lines": {"$sum": "$lines"}
                }
            },
            {"$sort": {"lines": -1}},
            { "$group": 
                {
                "_id": "$_id.play",
                "character": {"$first": "$_id.character"},
                "lines": { "$first": "$lines" },
                }
            },
            {"$project":
                {"play": "$_id",
                "character": "$character",
                "number of lines": "$lines"
                }
            }
            ]

pprint(list(shakespeare.aggregate(pipeline)))







