# crud.py
# Naim Lindsay
from pymongo import MongoClient
from bson.objectid import ObjectId


class AnimalShelter:
    """CRUD operations for AAC 'animals' collection + rescue-type query helper."""

    def __init__(self, username, password):
        # ---- Apporto connection 
        HOST = "nv-desktop-services.apporto.com"
        PORT = 34352
        DB = "AAC"
        COL = "animals"

        #  authSource to prevent auth failures in this Apporto
        mongo_uri = f"mongodb://{username}:{password}@{HOST}:{PORT}/{DB}?authSource={DB}"
        self.client = MongoClient(mongo_uri, serverSelectionTimeoutMS=4000)
        self.database = self.client[DB]
        self.collection = self.database[COL]

    # =========================
    # C - Create
    # =========================
    def create(self, data):
        """
        Insert a single document.
        Returns True/False for acknowledged write.
        """
        if not data:
            raise ValueError("Nothing to insert, data is empty.")
        result = self.collection.insert_one(data)
        return result.acknowledged

    # =========================
    # R - Read (generic)
    # =========================
    def read(self, query, projection=None, limit=None):
        """
        Generic read with optional projection and limit.
        Returns a list of documents.
        """
        try:
            cursor = self.collection.find(query, projection or {})
            if limit:
                cursor = cursor.limit(int(limit))
            return list(cursor)
        except Exception as e:
            print(f"Error during read: {e}")
            return []

    def read_all(self, projection=None, limit=500):
        """
        Convenience method to read all documents (optionally projected) with a limit.
        """
        try:
            cursor = self.collection.find({}, projection or {}).limit(int(limit))
            return list(cursor)
        except Exception as e:
            print(f"Error during read_all: {e}")
            return []

    # =========================
    # U - Update
    # =========================
    def update(self, query, new_values):
        """
        Update many documents matching 'query' using {"$set": new_values}.
        Returns the number of modified documents.
        """
        try:
            result = self.collection.update_many(query, {"$set": new_values})
            return result.modified_count
        except Exception as e:
            print(f"Error during update: {e}")
            return 0

    # =========================
    # D - Delete
    # =========================
    def delete(self, query):
        """
        Delete many documents matching 'query'.
        Returns the number of deleted documents.
        """
        try:
            result = self.collection.delete_many(query)
            return result.deleted_count
        except Exception as e:
            print(f"Error during delete: {e}")
            return 0

    # =====================================================
    # Rescue-specific query helper (Project Two requirement)
    # Matches the Dashboard Specifications PDF exactly:
    #   - Breedss
    #   - Sex ("Intact Male"/"Intact Female")
    #   - Age ranges (in weeks)
    # =====================================================
    BREEDS_BY_RESCUE = {
        "Water Rescue": {
            "breeds": [
                "Labrador Retriever Mix",
                "Chesapeake Bay Retriever",
                "Newfoundland",
            ],
            "sex": "Intact Female",
            "age_min": 26,
            "age_max": 156,
        },
        "Mountain or Wilderness Rescue": {
            "breeds": [
                "German Shepherd",
                "Alaskan Malamute",
                "Old English Sheepdog",
                "Siberian Husky",
                "Rottweiler",
            ],
            "sex": "Intact Male",
            "age_min": 26,
            "age_max": 156,
        },
        "Disaster or Individual Tracking": {
            "breeds": [
                "Doberman Pinscher",
                "German Shepherd",
                "Golden Retriever",
                "Bloodhound",
                "Rottweiler",
            ],
            "sex": "Intact Male",
            "age_min": 20,
            "age_max": 300,
        },
    }

    def query_for_rescue_type(self, rescue_type):
        """
        Build a MongoDB query for the given rescue type.
        Returns {} when rescue_type is not recognized (used for All / Reset).
        """
        spec = self.BREEDS_BY_RESCUE.get(rescue_type)
        if not spec:
            return {}  # No filter → All records

        return {
            "$and": [
                {"animal_type": "Dog"},
                {"breed": {"$in": spec["breeds"]}},
                {"sex_upon_outcome": spec["sex"]},
                {
                    "age_upon_outcome_in_weeks": {
                        "$gte": spec["age_min"],
                        "$lte": spec["age_max"],
                    }
                },
            ]
        }


