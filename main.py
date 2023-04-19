# Daniel Vahey, Ryan Chapados, Mark Hurley, Jake Fuiman, Ethan Gu
# DS4300 Final Project

from neo4j import GraphDatabase
from math import radians, cos, sin, asin, sqrt
#Load the data
""":auto LOAD CSV WITH HEADERS FROM 'file:///Users/rmons/School/neo4j_data/final/yelp_business_clean.csv' AS re
MERGE (r:Restaurant {id:re.business_id, lat:re.latitude, lon:re.longitude, name:re.name, review_count:re.review_count, stars:re.stars})
SET r.address = CASE trim(re.address) WHEN "" THEN null ELSE re.address END"""

#Load the edges
""":auto LOAD CSV WITH HEADERS FROM 'file:///Users/rmons/School/neo4j_data/final/edges.csv' AS edges
CALL {
WITH edges
MATCH (r1:Restaurant {id: edges.n1}), (r2:Restaurant {id: edges.n2})
CREATE (r1)-[:Similar_To {score: edges.score}]->(r2)
} IN TRANSACTIONS OF 2 ROWS"""


def connect(uname, pword):
    URI = "bolt://localhost:7687"
    auth = (uname, pword)

    driver = GraphDatabase.driver(URI, auth=auth)
    rslt = driver.verify_connectivity()

    return driver, rslt


def haversine(lat1, lon1, lat2, lon2):
    """
    Calculate the great circle distance in kilometers between two points
    on the earth (specified in decimal degrees)
    """
    # convert decimal degrees to radians
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])

    # haversine formula
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
    c = 2 * asin(sqrt(a))
    r = 3956  # Radius of earth in kilometers. Use 3956 for miles, or 6371 for km. Determines return value units.
    return c * r


# Gets a list of recommendations using the given restaurant name
def get_recommendations(driver, rest_name):
    rec_list = []
    with driver.session() as session:
        recs = session.run(
            "match (r1:Restaurant {name: '" + rest_name + "'})-[:Similar_To]->(r2:Restaurant) return r1, r2")

        for item in recs:
            if not rec_list:
                rec_list.append(dict(item.values()[0].items()))

            rec_list.append(dict(item.values()[1].items()))

    return rec_list


# Sorts the list of recommendations based on star rating, then displays them
def sort_recs(recs):
    r1 = recs[0]
    recs_ = recs[1:]

    # Calculates distance from the given restaurant
    for rest in recs_:
        rest['distance'] = haversine(float(r1['lat']), float(r1['lon']), float(rest['lat']), float(rest['lon']))

    # Sorts the scores by stars and distance
    return sorted(recs_, key=lambda item: (item['stars'], -item['distance']), reverse=True)


# Prints the recs to the user
def print_recs(recs):
    for rec in recs:
        print(rec['name'] + ", Rating: " + rec['stars'] + ", Distance from Input: ", round(rec['distance'], 2), "miles")


def main():
    uname = input("Enter username: ")
    pword = input("Enter password: ")

    try:
        driver, rslt = connect(uname, pword)
        print("---------------------------------")
        print(" Connection to neo4j successful")
        print("---------------------------------")
        print(
            "This recommendation engine only works for restaurants in the Santa Barbara area. Some good starting " +
            "points are 'Finch & Fork', 'Su Casa Fresh Mexican Grill', and 'Chase Restaurant'")

        while True:
            # Takes the users input
            r1 = input("Enter a restaurant, or 'exit' to exit: ")
            print("----------------------------------")

            # Exits the program
            if r1 == 'exit':
                break

            # Queries the neo4j database to get the recommendations
            recs = get_recommendations(driver, r1)

            # Ensures there are some valid recommendations
            if not recs:
                print("That restaurant is not in the database")
            else:
                # Sorts and print the recommendations
                print_recs(sort_recs(recs))

            print("----------------------------------")

    except Exception as e:
        print("----------------------------------")
        print("Error: " + str(e))
        print("----------------------------------")


if __name__ == "__main__":
    main()
