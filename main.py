# Daniel Vahey, Ryan Chapados, Mark Hurley, Jake Fuiman, Ethan Gu
# DS4300 Final Project

from neo4j import GraphDatabase


def connect(uname, pword):

    URI = "http://localhost:7474"
    auth = (uname, pword)

    driver = GraphDatabase.driver(URI, auth=auth)
    rslt = driver.verify_connectivity()

    return driver, rslt

def main():

    uname = input("enter username")
    pword = input("enter password")


    try:
        driver, rslt = connect(uname, pword)
        print("---------------------------------")
        print(" Connection to neo4j successful")
        print("---------------------------------")
        print()

    except Exception as e:
        print("----------------------------------")
        print(" Connection to neo4j unsuccessful")
        print("----------------------------------")


if __name__ == "__main__":
    main()
