import psycopg2
from pymongo import MongoClient

# MongoDB connection
def get_mongodb_connection():
    MONGO_URI = "mongodb+srv://iheb:Kt7oZ4zOW4Fg554q@cluster0.5zmaqup.mongodb.net/"
    MONGO_DB = "PowerBi"
    MONGO_COLLECTION = "frontusers"
    
    client = MongoClient(MONGO_URI)
    mongo_db = client[MONGO_DB]
    collection = mongo_db[MONGO_COLLECTION]
    return collection

# PostgreSQL connection
def get_postgres_connection():
    return psycopg2.connect(dbname="DW_DigitalCook", user='postgres', password='admin', host='localhost', port='5432')

# Fetch client_pk from PostgreSQL
# Fetch client_pk from PostgreSQL
def get_client_pk(matricule):
    conn = get_postgres_connection()
    cur = conn.cursor()
    
    # Cast matricule to text if it's not already a string
    cur.execute("SELECT client_pk FROM dim_client WHERE matricule = %s::text", (matricule,))
    
    client_pk = cur.fetchone()
    cur.close()
    conn.close()
    
    return client_pk[0] if client_pk else None


# Generate a unique factCode
def generate_fact_code(existing_codes):
    if not existing_codes:
        return "fact0001"
    else:
        last_code = max([int(code[4:]) for code in existing_codes if code.startswith("fact")], default=0)
        new_code = last_code + 1
        return f"fact{str(new_code).zfill(4)}"

# Extract data from MongoDB and map client_fk (client_pk)
def extract_and_insert_fact():
    collection = get_mongodb_connection()
    
    # Extract data from MongoDB
    users = collection.find()

    # Connect to PostgreSQL for insertions
    conn = get_postgres_connection()
    cur = conn.cursor()

    # List to track existing fact codes
    existing_fact_codes = []

    # Loop through MongoDB users
    for user in users:
        if 'profile' in user:
            profile = user['profile']
            matricule = user.get("matricule")
            
            # Get the client_pk from PostgreSQL using matricule
            client_pk = get_client_pk(matricule)
            
            if client_pk:
                # Generate a new factCode
                fact_code = generate_fact_code(existing_fact_codes)
                existing_fact_codes.append(fact_code)  # Keep track of the fact codes used

                # Insert the profile data into fact_client_profile
                insert_query = """
                INSERT INTO fact_client_profile (client_fk, factCode)
                VALUES (%s, %s)
                """
                values = (client_pk, fact_code)
                
                cur.execute(insert_query, values)
    
    # Commit the changes and close the connection
    conn.commit()
    cur.close()
    conn.close()

# Call the function to execute the extraction and insertion
extract_and_insert_fact()
