from pymongo import MongoClient

# Σύνδεση με τη MongoDB
client = MongoClient("mongodb://localhost:27017/")
db = client.vehiclesData_db
collection = db.processed_vehicle_positions
raw_collection = db.raw_vehicle_positions

# Ορισμός χρονικής περιόδου
start_time = "14/09/2024 19:18:30"
end_time = "14/09/2024 19:19:35"

# Query 1: Ακμές με το μικρότερο πλήθος οχημάτων (vcount)
min_vcount = collection.find_one({
    "time": {"$gte": start_time, "$lte": end_time}
}, sort=[("vcount", 1)])['vcount']

smallest_vcount_edge = collection.find({
    "time": {"$gte": start_time, "$lte": end_time},
    "vcount": min_vcount
})

# Εκτύπωση αποτελέσματος για το μικρότερο πλήθος οχημάτων
print("Ακμές με το μικρότερο πλήθος οχημάτων:")
for edge in smallest_vcount_edge:
    print(f"Ακμή: {edge['link']}, Πλήθος οχημάτων: {edge['vcount']}")

# Query 2: Βρες τη μέγιστη μέση ταχύτητα
max_vspeed = collection.find_one({
    "time": {"$gte": start_time, "$lte": end_time}
}, sort=[("vspeed", -1)])['vspeed']

largest_vspeed_edges = collection.find({
    "time": {"$gte": start_time, "$lte": end_time},
    "vspeed": max_vspeed
})

# Εκτύπωση αποτελέσματος για τη μεγαλύτερη μέση ταχύτητα
print("Ακμές με τη μεγαλύτερη μέση ταχύτητα:")
for edge in largest_vspeed_edges:
    print(f"Ακμή: {edge['link']}, Μέση ταχύτητα: {edge['vspeed']}")

# Query 3: Υπολογισμός της μεγαλύτερης διαδρομής
# Εύρεση της μέγιστης και ελάχιστης θέσης για κάθε ακμή
pipeline = [
    {"$match": {"time": {"$gte": start_time, "$lte": end_time}}},
    {"$group": {
        "_id": "$link",
        "max_position": {"$max": "$position"},
        "min_position": {"$min": "$position"}
    }},
    {"$project": {
        "link": "$_id",
        "max_position": 1,
        "min_position": 1,
        "distance": {"$subtract": ["$max_position", "$min_position"]}
    }},
    {"$sort": {"distance": -1}}
]

largest_distance_edge = raw_collection.aggregate(pipeline).next()

# Εκτύπωση αποτελέσματος για τη μεγαλύτερη διαδρομή
print(f"Ακμή με τη μεγαλύτερη διαδρομή: {largest_distance_edge['link']}, Διαδρομή: {largest_distance_edge['distance']} μέτρα")

# Κλείσιμο της σύνδεσης με τη MongoDB
client.close()