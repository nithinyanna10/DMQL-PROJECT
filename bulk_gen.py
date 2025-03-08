#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import psycopg2
from faker import Faker
import random
from datetime import datetime, timedelta
import logging
from tqdm import tqdm  # For progress bar

# ✅ Configure Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# ✅ Create Faker Instance
fake = Faker()

# ✅ Connect to PostgreSQL
def connect_db():
    try:
        conn = psycopg2.connect(
            dbname="flights",
            user="postgres",
            password="1234",
            host="localhost",
            port="5432"
        )
        logging.info("Connected to the database.")
        return conn
    except psycopg2.Error as e:
        logging.error(f"Database connection error: {e}")
        raise

# ✅ Function to Bulk Insert Data
def bulk_insert(conn, query, data, batch_size=1000):
    try:
        with conn.cursor() as cur:
            total = len(data)
            for i in range(0, total, batch_size):
                batch = data[i:i + batch_size]
                cur.executemany(query, batch)
                conn.commit()
                logging.info(f"Inserted {min(i + batch_size, total)}/{total} records.")
    except psycopg2.Error as e:
        logging.error(f"Bulk insert error: {e}")
        conn.rollback()
        raise

# ✅ Generate Unique ISO Codes
def generate_unique_iso_codes(num):
    iso_codes = set()
    while len(iso_codes) < num:
        new_code = fake.country_code()
        if len(new_code) == 2:
            iso_codes.add(new_code)
    return list(iso_codes)

# ✅ Generate Countries
def generate_countries(conn, num):
    iso_codes = generate_unique_iso_codes(num)
    data = [(fake.country(), iso_codes[i]) for i in range(num)]
    bulk_insert(conn, "INSERT INTO Countries (country_name, iso_code) VALUES (%s, %s)", data)

# ✅ Generate Cities
def generate_cities(conn, num):
    with conn.cursor() as cur:
        cur.execute("SELECT country_id FROM Countries")
        country_ids = [row[0] for row in cur.fetchall()]
    data = [(fake.city(), random.choice(country_ids)) for _ in range(num)]
    bulk_insert(conn, "INSERT INTO Cities (city_name, country_id) VALUES (%s, %s)", data)

# ✅ Generate Airlines
def generate_airlines(conn, num):
    with conn.cursor() as cur:
        cur.execute("SELECT country_id FROM Countries")
        country_ids = [row[0] for row in cur.fetchall()]
    iata_codes = set(fake.unique.bothify(text='??') for _ in range(num))
    icao_codes = set(fake.unique.bothify(text='???') for _ in range(num))
    data = [(iata_codes.pop(), icao_codes.pop(), fake.company(), random.choice(country_ids)) for _ in range(num)]
    bulk_insert(conn, "INSERT INTO Airlines (iata_code, icao_code, airline_name, country_id) VALUES (%s, %s, %s, %s)", data)

# ✅ Generate Airports
def generate_airports(conn, num):
    with conn.cursor() as cur:
        cur.execute("SELECT city_id FROM Cities")
        city_ids = [row[0] for row in cur.fetchall()]
    data = [(fake.unique.bothify(text='???'), fake.unique.bothify(text='????'), fake.company() + " Airport", random.choice(city_ids)) for _ in range(num)]
    bulk_insert(conn, "INSERT INTO Airports (iata_code, icao_code, airport_name, city_id) VALUES (%s, %s, %s, %s)", data)

# ✅ Generate Aircraft
def generate_aircraft(conn, num):
    with conn.cursor() as cur:
        cur.execute("SELECT airline_id FROM Airlines")
        airline_ids = [row[0] for row in cur.fetchall()]
    data = [(fake.unique.bothify(text='?##??'), fake.company(), fake.bothify(text='###-???'), random.choice(airline_ids)) for _ in range(num)]
    bulk_insert(conn, "INSERT INTO Aircraft (registration, manufacturer, model, airline_id) VALUES (%s, %s, %s, %s)", data)

# ✅ Generate Routes
def generate_routes(conn, num):
    print("Generating routes...")
    
    # Fetch airport_ids from the database
    with conn.cursor() as cur:
        cur.execute("SELECT airport_id FROM Airports")
        airport_ids = [row[0] for row in cur.fetchall()]
    
    # Generate route data
    data = []
    for _ in tqdm(range(num), desc="Routes"):
        departure_airport_id = random.choice(airport_ids)
        arrival_airport_id = random.choice(airport_ids)
        distance_km = round(random.uniform(100, 10000), 2)
        estimated_flight_time = f"{random.randint(1,12)}h {random.randint(0,59)}m"
        
        data.append((departure_airport_id, arrival_airport_id, distance_km, estimated_flight_time))
    
    # Insert the data into the Routes table
    bulk_insert(conn, "INSERT INTO Routes (departure_airport_id, arrival_airport_id, distance_km, estimated_flight_time) VALUES (%s, %s, %s, %s)", data)
    print("Routes generated and inserted into the Routes table.\n")

# ✅ Generate Weather
def generate_weather(conn, num):
    with conn.cursor() as cur:
        cur.execute("SELECT airport_id FROM Airports")
        airport_ids = [row[0] for row in cur.fetchall()]
    data = [(random.choice(airport_ids), round(random.uniform(-20, 40), 2), round(random.uniform(0, 100), 2), round(random.uniform(0, 50), 2), fake.date_time_this_year()) for _ in range(num)]
    bulk_insert(conn, "INSERT INTO Weather (airport_id, temperature, wind_speed, precipitation, timestamp) VALUES (%s, %s, %s, %s, %s)", data)

# ✅ Generate Flights
def generate_flights(conn, num):
    print("Generating flights...")
    
    # Fetch airline_ids, aircraft_ids, airport_ids, route_ids, and weather_ids from the database
    with conn.cursor() as cur:
        cur.execute("SELECT airline_id FROM Airlines")
        airline_ids = [row[0] for row in cur.fetchall()]
        
        cur.execute("SELECT aircraft_id FROM Aircraft")
        aircraft_ids = [row[0] for row in cur.fetchall()]
        
        cur.execute("SELECT airport_id FROM Airports")
        airport_ids = [row[0] for row in cur.fetchall()]
        
        cur.execute("SELECT route_id FROM Routes")
        route_ids = [row[0] for row in cur.fetchall()]
        
        cur.execute("SELECT weather_id FROM Weather")
        weather_ids = [row[0] for row in cur.fetchall()]
    
    # Generate flight data
    data = []
    for _ in tqdm(range(num), desc="Flights"):
        flight_number = fake.unique.bothify(text='??###')
        airline_id = random.choice(airline_ids)
        aircraft_id = random.choice(aircraft_ids)
        departure_airport_id = random.choice(airport_ids)
        arrival_airport_id = random.choice(airport_ids)
        route_id = random.choice(route_ids)
        weather_id = random.choice(weather_ids)
        departure_time = fake.date_time_this_year()
        arrival_time = departure_time + timedelta(hours=random.randint(1, 12))
        flight_status = random.choice(['Scheduled', 'Delayed', 'Cancelled', 'Completed'])
        
        data.append((flight_number, airline_id, aircraft_id, departure_airport_id, arrival_airport_id, route_id, weather_id, departure_time, arrival_time, flight_status))
    
    # Insert the data into the Flights table
    bulk_insert(conn, "INSERT INTO Flights (flight_number, airline_id, aircraft_id, departure_airport_id, arrival_airport_id, route_id, weather_id, departure_time, arrival_time, flight_status) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", data)
    print("Flights generated and inserted into the Flights table.\n")

# ✅ Generate Crew
def generate_crew(conn, num):
    print("Generating crew...")
    
    # Fetch airline_ids from the database
    with conn.cursor() as cur:
        cur.execute("SELECT airline_id FROM Airlines")
        airline_ids = [row[0] for row in cur.fetchall()]
    
    # Generate crew data
    data = [(fake.first_name(), fake.last_name(), random.choice(['Pilot', 'Co-Pilot', 'Flight Attendant']), random.choice(airline_ids)) for _ in range(num)]
    
    # Insert the data into the Crew table
    bulk_insert(conn, "INSERT INTO Crew (first_name, last_name, role, airline_id) VALUES (%s, %s, %s, %s)", data)
    print("Crew generated and inserted into the Crew table.\n")

# ✅ Generate Flight Crew Assignments
def generate_flight_crew(conn, num):
    print("Generating flight crew assignments...")
    
    # Fetch flight_ids and crew_ids from the database
    with conn.cursor() as cur:
        cur.execute("SELECT flight_id FROM Flights")
        flight_ids = [row[0] for row in cur.fetchall()]
        
        cur.execute("SELECT crew_id FROM Crew")
        crew_ids = [row[0] for row in cur.fetchall()]
    
    # Use a set to ensure unique (flight_id, crew_id) pairs
    data = set()
    for _ in tqdm(range(num), desc="Flight Crew"):
        flight_id = random.choice(flight_ids)
        crew_id = random.choice(crew_ids)
        data.add((flight_id, crew_id))
    
    # Insert the data into the FlightCrew table
    bulk_insert(conn, "INSERT INTO FlightCrew (flight_id, crew_id) VALUES (%s, %s)", list(data))
    print("Flight crew assignments generated and inserted into the FlightCrew table.\n")

# ✅ Generate Passengers
def generate_passengers(conn, num):
    data = [(fake.first_name(), fake.last_name(), fake.date_of_birth(minimum_age=18, maximum_age=90), fake.country()[:50]) for _ in range(num)]
    bulk_insert(conn, "INSERT INTO Passengers (first_name, last_name, date_of_birth, nationality) VALUES (%s, %s, %s, %s)", data)

# ✅ Generate Tickets
def generate_tickets(conn, num):
    with conn.cursor() as cur:
        cur.execute("SELECT flight_id FROM Flights")
        flight_ids = [row[0] for row in cur.fetchall()]
        cur.execute("SELECT passenger_id FROM Passengers")
        passenger_ids = [row[0] for row in cur.fetchall()]
    data = [(random.choice(flight_ids), random.choice(passenger_ids), fake.bothify(text='?##'), round(random.uniform(50, 1000), 2), random.choice(['Economy', 'Business', 'First Class'])) for _ in range(num)]
    bulk_insert(conn, "INSERT INTO Tickets (flight_id, passenger_id, seat_number, ticket_price, ticket_class) VALUES (%s, %s, %s, %s, %s)", data)

# ✅ Generate Baggage
def generate_baggage(conn, num):
    with conn.cursor() as cur:
        cur.execute("SELECT passenger_id FROM Passengers")
        passenger_ids = [row[0] for row in cur.fetchall()]
    data = [(random.choice(passenger_ids), round(random.uniform(5, 30), 2), random.choice(['Checked', 'Carry-on'])) for _ in range(num)]
    bulk_insert(conn, "INSERT INTO Baggage (passenger_id, weight, bag_type) VALUES (%s, %s, %s)", data)

# ✅ Generate Fuel Consumption
def generate_fuel_consumption(conn, num):
    with conn.cursor() as cur:
        cur.execute("SELECT flight_id FROM Flights")
        flight_ids = [row[0] for row in cur.fetchall()]
    data = [(random.choice(flight_ids), round(random.uniform(1000, 50000), 2), round(random.uniform(1000, 50000) * 2.31, 2)) for _ in range(num)]
    bulk_insert(conn, "INSERT INTO FuelConsumption (flight_id, fuel_used_liters, carbon_emission) VALUES (%s, %s, %s)", data)

# ✅ Generate Incidents
def generate_incidents(conn, num):
    with conn.cursor() as cur:
        cur.execute("SELECT flight_id FROM Flights")
        flight_ids = [row[0] for row in cur.fetchall()]
    data = [(random.choice(flight_ids), random.choice(['Technical Issue', 'Medical Emergency', 'Security Threat', 'Weather Delay']), fake.text(max_nb_chars=200), fake.date_time_this_year()) for _ in range(num)]
    bulk_insert(conn, "INSERT INTO Incidents (flight_id, incident_type, description, timestamp) VALUES (%s, %s, %s, %s)", data)

# ✅ Generate Flight Routes
def generate_flight_routes(conn, num):
    print("Generating flight routes...")
    
    # Fetch flight_ids and route_ids from the database
    with conn.cursor() as cur:
        cur.execute("SELECT flight_id FROM Flights")
        flight_ids = [row[0] for row in cur.fetchall()]
        
        cur.execute("SELECT route_id FROM Routes")
        route_ids = [row[0] for row in cur.fetchall()]
    
    # Generate flight route data
    data = [(random.choice(flight_ids), random.choice(route_ids)) for _ in range(num)]
    
    # Insert the data into the FlightRoutes table
    bulk_insert(conn, "INSERT INTO FlightRoutes (flight_id, route_id) VALUES (%s, %s)", data)
    print("Flight routes generated and inserted into the FlightRoutes table.\n")

# ✅ Main Function
def main():
    conn = connect_db()
    try:
        generate_countries(conn, 100)
        generate_cities(conn, 1000)
        generate_airlines(conn, 500)
        generate_airports(conn, 5000)
        generate_aircraft(conn, 10000)
        generate_routes(conn, 50000)  # Generate routes first
        generate_weather(conn, 100000)
        generate_flights(conn, 100000)  # Generate flights next
        generate_crew(conn, 5000)
        generate_flight_crew(conn, 10000)
        generate_passengers(conn, 50000)
        generate_tickets(conn, 100000)
        generate_baggage(conn, 100000)
        generate_fuel_consumption(conn, 100000)
        generate_incidents(conn, 10000)
        generate_flight_routes(conn, 100000)  # Generate flight routes last
    finally:
        conn.close()
        logging.info("Database connection closed.")

if __name__ == "__main__":
    main()

