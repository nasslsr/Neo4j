from neo4j import GraphDatabase
import pandas as pd

uri = "bolt://44.199.252.109:7687"  
password='beginner-shows-stairs'
driver = GraphDatabase.driver(uri, auth=("neo4j", password)) 


#Creation des noeuds
def add_airport_node(tx, id, name, city, country, iata, icao, latitude, longitude, altitude):
    tx.run("CREATE (:Airporttt {id: $id, name: $name, city: $city, country: $country, iata: $iata, icao: $icao, latitude: $latitude, longitude: $longitude, altitude: $altitude})",
           id=id, name=name, city=city, country=country, iata=iata, icao=icao, latitude=latitude, longitude=longitude, altitude=altitude)

def add_route_node(tx, airline, airline_id, source_id, departure_iata, dest_id, arrival_iata, codeshare, stops, equipment):
    tx.run("CREATE (:Route {airline: $airline, airline_id: $airline_id, source_id: $source_id, departure_iata: $departure_iata, dest_id: $dest_id, arrival_iata: $arrival_iata, codeshare: $codeshare, stops: $stops, equipment: $equipment})",
           airline=airline, airline_id=airline_id, source_id=source_id, departure_iata=departure_iata, dest_id=dest_id, arrival_iata=arrival_iata, codeshare=codeshare, stops=stops, equipment=equipment)


airports_df = pd.read_csv('airports.csv', header=None)
routes_df = pd.read_csv('routes.csv', header=None)

with driver.session() as session:
    for index, row in airports_df.iterrows():
        session.execute_write(add_airport_node, row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8])

    for index, row in routes_df.iterrows():
        session.execute_write(add_route_node, row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8])

        
# Creation des relations departs from et arrived at         
def create_relations(tx, start_node=0, nombre_noeuds_a_traiter=3000):
    tx.run("""
    MATCH (departure:Airporttt), (arrival:Airporttt), (route:Route)
    WHERE departure.iata = route.departure_iata AND arrival.iata = route.arrival_iata
    WITH departure, arrival, route
    SKIP $start_node
    LIMIT $nombre_noeuds_a_traiter
    CREATE (departure)-[:DEPARTS_FROM]->(route)-[:ARRIVES_AT]->(arrival)
    """, start_node=start_node, nombre_noeuds_a_traiter=nombre_noeuds_a_traiter)


nombre_total_noeuds = 60000
nombre_noeuds_par_lot = 1000

with driver.session() as session:
    for start_node in range(0, nombre_total_noeuds, nombre_noeuds_par_lot):
        session.execute_write(create_relations, start_node=start_node, nombre_noeuds_a_traiter=nombre_noeuds_par_lot)


# Ajouter le nom des compagnies aerienne aux id airline correspondant
airlines_df = pd.read_csv('airlines.csv', usecols=[0, 1], header=None, encoding='utf-8')

with driver.session() as session:
    for index, row in airlines_df.iterrows():
        airline_id = str(row[0])
        airline_name = row[1]

        cypher_query = """
        MATCH (r:Route {airline_id: $airline_id})
        SET r.airline_name = $airline_name
        RETURN r
        """

        result = session.run(cypher_query, airline_id=airline_id, airline_name=airline_name)

        if result.consume().counters.properties_set > 0:
            print(f"Airline ID: {airline_id} - Airline Name: {airline_name} ajouté avec succès.")
        else:
            print(f"Aucun nœud correspondant pour Airline ID: {airline_id} - Airline Name: {airline_name}.")


# Recherche entre France et Algerie

def get_routes(tx):
    result = tx.run("""
    MATCH (departure:Airporttt {country: "France"})-[:DEPARTS_FROM]->(route:Route)-[:ARRIVES_AT]->(arrival:Airporttt {country: "Algeria"})
RETURN departure.name AS Depart, arrival.name AS Arrive,route.airline_id AS Numero_de_vol
LIMIT 20
    """)
    return result.data()

with driver.session() as session:
    routes = session.execute_read(get_routes)

for route in routes:
    print(route)



#Entrer une ville de départ, une ville d’arrivée et obtenir ensuite la liste des vols
#permettant d’effectuer ce trajet, en proposant prioritairement les vols directs


def get_routes_by_city(tx, departure_city, arrival_city):
    query = """
    MATCH (departure:Airporttt { city: $departure_city})-[:DEPARTS_FROM]->(route:Route)-[:ARRIVES_AT]->(arrival:Airporttt { city: $arrival_city})
    RETURN departure.name AS Departure, arrival.name AS Arrival, route.airline_name AS Compagny, route.airline_id AS AirlineID, route.stops AS Stops
    ORDER BY route.stops
    LIMIT 100
    """
    result = tx.run(query, departure_city=departure_city, arrival_city=arrival_city)
    return result.data()

departure_city = input("Veuillez entrer la ville de départ : ")
arrival_city = input("Veuillez entrer la ville d'arrivée : ")

with driver.session() as session:
    routes = session.execute_read(get_routes_by_city, departure_city, arrival_city)

for route in routes:
    print(route)

# Pouvoir raffiner la recherche en spécifiant une compagnie aérienne privilégiée et/ou une escale souhaitée

def get_routes_by_city(tx, departure_city, arrival_city, compagny=None, escale=None):
    query = """
    MATCH (departure:Airporttt { city: $departure_city})-[:DEPARTS_FROM]->(route:Route)-[:ARRIVES_AT]->(arrival:Airporttt { city: $arrival_city})
    WHERE ($preferred_airline IS NULL OR route.airline_name = $compagny)
    AND ($escale IS NULL OR EXISTS((route)-[:ARRIVES_AT]->(:Airporttt { city: $escale })))
    RETURN departure.name AS Departure, arrival.name AS Arrival, route.airline_name AS Compagny, route.airline_id AS AirlineID, route.stops AS Stops
    ORDER BY route.stops
    LIMIT 20
    """
    result = tx.run(query, departure_city=departure_city, arrival_city=arrival_city, compagny=compagny, escale=escale)
    return result.data()

departure_city = input("Veuillez entrer la ville de départ : ")
arrival_city = input("Veuillez entrer la ville d'arrivée : ")
compagny = input("Veuillez entrer la compagnie aérienne privilégiée (ou laissez vide pour ignorer) : ")
escale = input("Veuillez entrer une escale souhaitée (ou laissez vide pour ignorer) : ")

with driver.session() as session:
    routes = session.execute_read(get_routes_by_city, departure_city, arrival_city, compagny, escale)

for route in routes:
    print(route)



#Recherche entre pays

def get_routes_by_country(tx, departure_country, arrival_country):
    query = """
    MATCH (departure:Airporttt { country: $departure_country})-[:DEPARTS_FROM]->(route:Route)-[:ARRIVES_AT]->(arrival:Airporttt { country: $arrival_country})
    RETURN departure.name AS Departure, arrival.name AS Arrival, route.airline_id AS FlightNumber, route.airline_name AS Compagny
    LIMIT 20
    """
    result = tx.run(query, departure_country=departure_country, arrival_country=arrival_country)
    return result.data()

departure_country = input("Veuillez entrer le pays de départ : ")
arrival_country = input("Veuillez entrer le pays d'arrivée : ")

with driver.session() as session:
    routes = session.execute_read(get_routes_by_country, departure_country, arrival_country)
for route in routes:
    print(route)


#TOUR DU MONDE


def tour_du_monde(tx, departure_city, arrival_city):
    query = """
        MATCH (departure:Airporttt { city: $departure_city })-[:DEPARTS_FROM]->(route:Route)-[:ARRIVES_AT]->(arrival:Airporttt { city: $arrival_city })
        RETURN departure.name AS Departure, arrival.name AS Arrival, route.airline_name AS Compagny, route.airline_id AS AirlineID, route.stops AS Stops
        ORDER BY route.stops
        LIMIT 5
    """
    result = tx.run(query, departure_city=departure_city, arrival_city=arrival_city)
    return result.data()

itineraires = [('Paris', 'Tokyo'), ('Tokyo', 'Winnipeg'), ('Winnipeg', 'Diffa'), ('Diffa', 'Bou Sfer'), ('Bou Sfer', 'Bizerte'), ('Bizerte', 'Paris')]

with driver.session() as session:
    previous_route = None
    for itinerary in itineraires:
        departure_city, arrival_city = itinerary
        routes = session.execute_read(tour_du_monde, departure_city, arrival_city)
        for route in routes:
            if route != previous_route:
                print(route)
                previous_route = route
