import pandas as pd
from faker import Faker
import random
from datetime import datetime, timedelta
import uuid
from dotenv import load_dotenv
import os
import psycopg2
from data_operations import connect_with_connector
from sqlalchemy import text

fake = Faker()

# print(fake.name())


engine = connect_with_connector()

# print("Connected to DB:", os.getenv('POSTGRES_DB'))

def generate_drivers( n=100):
    car_types = ["sedan", "hatchback", "SUV", "luxury", "2-seater", "10-seater", "van", "pickup truck"]
    
    drivers = [
        {
            "name": fake.name(),
            "phone": fake.phone_number(),
            "email": fake.email(),
            "sex": random.choice(["Male", "Female"]),
            "car_model": random.choice(car_types),
            "car_number": fake.license_plate(),
            "driver_license_number": fake.uuid4()[:10],
            "driver_experience": random.randint(1, 10),
            "driver_rating": round(random.uniform(3.0, 5.0), 2),
            "created_at": datetime.now()
        }
        for _ in range(n)
    ]
    
    insert_query = text("""
        INSERT INTO driver (name, phone, email, sex, car_model, car_number, driver_license_number, driver_experience, driver_rating, created_at) 
        VALUES (:name, :phone, :email, :sex, :car_model, :car_number, :driver_license_number, :driver_experience, :driver_rating, :created_at)
    """)

    # Insert using SQLAlchemy
    with engine.connect() as connection:
        connection.execute(insert_query, drivers)
        connection.commit()
    
    print(f"{n} drivers inserted successfully!")
    
def generate_users(n=100):
    users = []
    for _ in range(n):
        users.append({
            "name": fake.name(),
            "phone": str(fake.phone_number()),
            "email": fake.email(),
            "sex": random.choice(["Male", "Female"]),
            "is_active": str(random.choice([True, False])),  # is_active as string
            "user_rating": float(round(random.uniform(2.0, 5.0), 2)),  # Ensure float
            "created_at": datetime.now()
        })

    insert_query = text("""
        INSERT INTO users (name, phone, email, sex, is_active, user_rating, created_at) 
        VALUES (:name, :phone, :email, :sex, :is_active, :user_rating, :created_at)
    """)

    with engine.connect() as connection:
        connection.execute(insert_query, users)  # ✅ Now users is a list of dictionaries
        connection.commit()

    
    print(f"{n} users inserted successfully!")

def generate_payments(n=500):
    payments = [
        {
            "amount": round(random.uniform(5.0, 100.0), 2),
            "payment_status": random.choice(["pending", "successful", "failed"]),
            "payment_method": random.choice(["credit_card", "debit_card", "wallet", "cash"]),
            "created_at": fake.date_time_this_year()
        }
        for _ in range(n)
    ]

    insert_query = text("""
        INSERT INTO payments (amount, payment_status, payment_method, created_at)
        VALUES (:amount, :payment_status, :payment_method, :created_at)
    """)

    with engine.connect() as connection:
        connection.execute(insert_query, payments)  # ✅ List of dictionaries
        connection.commit()

def generate_locations(n=200):
    locations = [
        {
            "city": fake.city(),
            "state": fake.state(),
            "country": fake.country(),
            "latitude": round(random.uniform(-90, 90), 6),
            "longitude": round(random.uniform(-180, 180), 6)
        }
        for _ in range(n)
    ]

    insert_query = text("""
        INSERT INTO locations (city, state, country, latitude, longitude)
        VALUES (:city, :state, :country, :latitude, :longitude)
    """)

    with engine.connect() as connection:
        connection.execute(insert_query, locations)  # ✅ List of dictionaries
        connection.commit()
    
    print(f"{n} location records inserted successfully.")


def generate_rides(users, payments, drivers, n=300):
    rides = [
        {
            "user_id": random.choice(users),
            "driver_id": random.choice(drivers),
            "payment_id": random.choice(payments),
            "pickup_latitude": round(random.uniform(12.9, 77.9), 6),
            "pickup_longitude": round(random.uniform(12.9, 77.9), 6),
            "dropoff_latitude": round(random.uniform(12.9, 77.9), 6),
            "dropoff_longitude": round(random.uniform(12.9, 77.9), 6),
            "ride_status": random.choice(["completed", "cancelled", "in_progress"]),
            "start_time": fake.date_time_this_year(),
            "end_time": fake.date_time_this_year(),
            "distance_km": round(random.uniform(0.5, 50.0), 2),
            "duration_sec": random.randint(120, 3600),
            "fare_amount": round(random.uniform(5.0, 100.0), 2),
            "passenger_count": random.randint(1, 6),
            "rating": round(random.uniform(3.0, 5.0), 2),
            "created_at": fake.date_time_this_year()
        }
        for _ in range(n)
    ]

    insert_query = text("""
        INSERT INTO rides (user_id, driver_id, payment_id, pickup_latitude, pickup_longitude, dropoff_latitude,
                           dropoff_longitude, ride_status, start_time, end_time, distance_km, duration_sec, 
                           fare_amount, passenger_count, rating, created_at)
        VALUES (:user_id, :driver_id, :payment_id, :pickup_latitude, :pickup_longitude, :dropoff_latitude, 
                :dropoff_longitude, :ride_status, :start_time, :end_time, :distance_km, :duration_sec, 
                :fare_amount, :passenger_count, :rating, :created_at)
    """)

    with engine.connect() as connection:
        connection.execute(insert_query, rides)
        connection.commit()

    print(f"{n} rides inserted successfully.")

def generate_pricing(rides, n=300):
    pricing = [
        {
            "ride_id": random.choice(rides),
            "base_fare": round(random.uniform(5.0, 20.0), 2),
            "surge_multiplier": round(random.uniform(1.0, 3.0), 2),
            "final_fare": round(random.uniform(5.0, 20.0), 2) * round(random.uniform(1.0, 3.0), 2),
            "created_at": fake.date_time_this_year()
        }
        for _ in range(n)
    ]

    insert_query = text("""
        INSERT INTO pricing (ride_id, base_fare, surge_multiplier, final_fare, created_at)
        VALUES (:ride_id, :base_fare, :surge_multiplier, :final_fare, :created_at)
    """)

    with engine.connect() as connection:
        connection.execute(insert_query, pricing)
        connection.commit()

    print(f"{n} pricing records inserted successfully.")

def generate_cancellations(rides, n=100):
    cancellations = [
        {
            "ride_id": random.choice(rides),
            "cancelled_by": random.choice(["rider", "driver", "system"]),
            "cancel_reason": fake.sentence(),
            "created_at": fake.date_time_this_year()
        }
        for _ in range(n)
    ]

    insert_query = text("""
        INSERT INTO cancellations (ride_id, cancelled_by, cancel_reason, created_at)
        VALUES (:ride_id, :cancelled_by, :cancel_reason, :created_at)
    """)

    with engine.connect() as connection:
        connection.execute(insert_query, cancellations)
        connection.commit()

    print(f"{n} cancellations inserted successfully.")

def generate_driver_availability( drivers, locations, n=500):
    driver_availabilities = [
        {
            "driver_id": random.choice(drivers),
            "location_id": random.choice(locations),
            "status": random.choice(["AVAILABLE", "UNAVAILABLE"]),
            "created_at": fake.date_time_this_year()
        }
        for _ in range(n)
    ]

    insert_query = text("""
        INSERT INTO driver_availablity (driver_id, location_id, status, created_at)
        VALUES (:driver_id, :location_id, :status, :created_at)
    """)

    with engine.connect() as connection:
        connection.execute(insert_query, driver_availabilities)
        connection.commit()

    print(f"{n} driver availability records inserted successfully.")



# Execute data generation in order
def get_data(query):
    print(query)
    with engine.connect() as connection:
        result = connection.execute(text(query))
        print('after execute')
        return [row[0] for row in result.fetchall()]


def auto_insert_data():
    users = get_data("SELECT id FROM users")
    payments = get_data("SELECT id FROM payments")
    drivers = get_data("SELECT id FROM driver")
    locations = get_data("SELECT id FROM locations")

    
    generate_rides(users, payments,drivers, 100)
    rides = get_data("SELECT id FROM rides")
    generate_cancellations(rides, 100)
    generate_pricing(rides, 300)
    generate_driver_availability(drivers,locations, 5)


if __name__ == "__main__":
    auto_insert_data()




