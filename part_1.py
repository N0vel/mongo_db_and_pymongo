import pymongo
from datetime import datetime

client = pymongo.MongoClient('mongodb://localhost:27017/')
database = client.my_database
database.persons_collection.drop()
persons = database.persons_collection
docs = [{'person_id': 0,
         'first name': 'Mike',
         'last name': 'Wazowski',
         'workplace': 'Monsters, Inc.',
         'phone number': '+19039615830',
         'birthday': datetime(1996, 5, 13),
         'hobby': 'Scaring',
         'Favorite color': 'Green'},
         {'person_id': 1,
         'first name': 'John',
         'last name': 'Connor',
         'workplace': '',
         'phone number': '+19031828330',
         'birthday': datetime(1970, 3, 23),
         'hobby': 'Playing guitar',
         'Favorite color': 'Black'},
         {'person_id': 2,
         'first name': 'Freddie',
         'last name': 'Mercury',
         'workplace': 'Queen',
         'phone number': '+19641920593',
         'birthday': datetime(1946, 9, 5),
         'hobby': 'Signing',
         'Favorite color': 'Yellow'},
         {'person_id': 3,
         'first name': 'Tony',
         'last name': 'Stark',
         'workplace': 'Stark Industries',
         'phone number': '+19632018539',
         'birthday': datetime(1970, 5, 29),
         'hobby': 'Inventing',
         'Favorite color': 'Purple'}]

persons.insert_many(docs)
print(persons.count_documents({})) 


persons.create_index([('person_id', pymongo.ASCENDING)], unique=True)
persons.create_index([('first name', pymongo.TEXT), ('last name', pymongo.TEXT)], name='search_by_name')

print(sorted(list(database.persons_collection.index_information())))

new_person = {'person_id': 4,
            'first name': 'John',
            'last name': 'Doe',
            'workplace': 'Apple Inc.',
            'phone number': '+19648760593',
            'birthday': datetime(1986, 3, 11),
            'hobby': 'Driving',
            'Favorite color': 'Cyan'}

# CRUD

# Create
persons.insert_one(new_person)
print(persons.count_documents({})) 

# Read
doe = persons.find_one({'last name': 'Doe'})
print(doe)

# Update
persons.update({'person_id': 4}, {"$set": {"first name": "Alister"}}, upsert=True)
print(persons.find_one({'last name': 'Doe'}))

# Delete
persons.delete_one({"first name": "Mike"})
print(persons.count_documents({}))




# Aggregation
pipeline = [{"$group": {"_id": 
            {"month": {"$month": "$birthday"}        
            },
            "count": {"$sum": 1}
            }},
            {"$match": {"_id": {"month": datetime.today().month}}}
            ]

print(list(persons.aggregate(pipeline)))

# Map/Reduce
print('Map/Reduce')
from bson.code import Code
mapper = Code("""
               function () {
                 if (this.birthday.getMonth() == %d) 
                 { 
                     emit(1, 1);
                 }
            }
               """ % int(datetime.today().month - 1))


reducer = Code("""
                function (key, values) {
                    var count = 0;
                    values.forEach(function(v) { count++ } );
                    return count;
                }
                """)

result = persons.map_reduce(mapper, reducer, "myresults", full_response=False)
for doc in result.find():
    print(doc)








