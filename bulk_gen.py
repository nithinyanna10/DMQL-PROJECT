#!/usr/bin/env python
# coding: utf-8

import os
import psycopg2
from faker import Faker
import random
from datetime import datetime, timedelta
import logging
from tqdm import tqdm  # For progress bar
from dotenv import load_dotenv

# ✅ Load environment variables
load_dotenv()

# ✅ Configure Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# ✅ Create Faker Instance
fake = Faker()

# ✅ Connect to PostgreSQL
def connect_db():
    try:
        conn = psycopg2.connect(
            dbname=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            host=os.getenv("DB_HOST"),
            port=os.getenv("DB_PORT")
        )
        logging.info("Connected to the database.")
        return conn
    except psycopg2.Error as e:
        logging.error(f"Database connection error: {e}")
        raise

# ✅ Bulk Insert Helper
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
    with conn.cursor() as cur:
        cur.execute("SELECT airport_id FROM Airports")
        airport_ids = [row[0] for row in cur.fetchall()]
    data = []
    for _ in tqdm(range(num), desc="Routes"):
        dep_id, arr_id = random.sample(airport_ids, 2)
        distance = round(random.uniform(100, 10000), 2)
        est_time = f"{random.randint(1, 12)}h {random.randint(0, 59)}m"
        data.append((dep_id, arr_id, distance, est_time))
    bulk_insert(conn, "INSERT INTO Routes (departure_airport_id, arrival_airport_id, distance_km, estimated_flight_time) VALUES (%s, %s, %s, %s)", data)

# ✅ Generate Weather
def generate_weather(conn, num):
    with conn.cursor() as cur:
        cur.execute("SELECT airport_id FROM Airports")
        airport_ids = [row[0] for row in cur.fetchall()]
    data = [(random.choice(airport_ids), round(random.uniform(-20, 40), 2), round(random.uniform(0, 100), 2), round(random.uniform(0, 50), 2), fake.date_time_this_year()) for _ in range(num)]
    bulk_insert(conn, "INSERT INTO Weather (airport_id, temperature, wind_speed, precipitation, timestamp) VALUES (%s, %s, %s, %s, %s)", data)

# ✅ Generate Flights
def generate_flights(conn, num):
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
    data = []
    for _ in tqdm(range(num), desc="Flights"):
        dep_time = fake.date_time_this_year()
        arr_time = dep_time + timedelta(hours=random.randint(1, 12))
        data.append((
            fake.unique.bothify(text='??###'),
            random.choice(airline_ids),
            random.choice(aircraft_ids),
            random.choice(airport_ids),
            random.choice(airport_ids),
            random.choice(route_ids),
            random.choice(weather_ids),
            dep_time,
            arr_time,
            random.choice(['Scheduled', 'Delayed', 'Cancelled', 'Completed'])
        ))
    bulk_insert(conn, """INSERT INTO Flights (
        flight_number, airline_id, aircraft_id, departure_airport_id, 
        arrival_airport_id, route_id, weather_id, departure_time, 
        arrival_time, flight_status) 
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""", data)

# ✅ Generate Crew
def generate_crew(conn, num):
    with conn.cursor() as cur:
        cur.execute("SELECT airline_id FROM Airlines")
        airline_ids = [row[0] for row in cur.fetchall()]
    data = [(fake.first_name(), fake.last_name(), random.choice(['Pilot', 'Co-Pilot', 'Flight Attendant']), random.choice(airline_ids)) for _ in range(num)]
    bulk_insert(conn, "INSERT INTO Crew (first_name, last_name, role, airline_id) VALUES (%s, %s, %s, %s)", data)

# ✅ Generate Flight Crew Assignments
def generate_flight_crew(conn, num):
    with conn.cursor() as cur:
        cur.execute("SELECT flight_id FROM Flights")
        flight_ids = [row[0] for row in cur.fetchall()]
        cur.execute("SELECT crew_id FROM Crew")
        crew_ids = [row[0] for row in cur.fetchall()]
    data = set()
    for _ in tqdm(range(num), desc="FlightCrew"):
        data.add((random.choice(flight_ids), random.choice(crew_ids)))
    bulk_insert(conn, "INSERT INTO FlightCrew (flight_id, crew_id) VALUES (%s, %s)", list(data))

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
        cur.execute("SELECT ticket_id FROM Tickets")
        ticket_ids = [row[0] for row in cur.fetchall()]
    data = [(random.choice(ticket_ids), round(random.uniform(5, 40), 2), random.choice(['Checked', 'Cabin'])) for _ in range(num)]
    bulk_insert(conn, "INSERT INTO Baggage (ticket_id, weight, baggage_type) VALUES (%s, %s, %s)", data)

# ✅ Main
if __name__ == "__main__":
    conn = connect_db()

    generate_countries(conn, 20)
    generate_cities(conn, 50)
    generate_airlines(conn, 10)
    generate_airports(conn, 30)
    generate_aircraft(conn, 20)
    generate_routes(conn, 50)
    generate_weather(conn, 100)
    generate_flights(conn, 100)
    generate_crew(conn, 50)
    generate_flight_crew(conn, 200)
    generate_passengers(conn, 100)
    generate_tickets(conn, 150)
    generate_baggage(conn, 100)

    conn.close()
    logging.info("All data generated successfully!")
