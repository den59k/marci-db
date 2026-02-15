#!/usr/bin/env python3
"""
Script to populate MarciDB with test data.
This script creates multiple objects for each model with proper relationships.
"""

import requests
import json
import random
from datetime import datetime, timedelta
from typing import Dict, List, Any

# Configuration
BASE_URL = "http://127.0.0.1:3000"

# Sample data for generation
FIRST_NAMES = [
    "Alice", "Bob", "Charlie", "Diana", "Eva", "Frank", "Grace", "Henry",
    "Iris", "Jack", "Kate", "Leo", "Maya", "Noah", "Olivia", "Paul"
]

LAST_NAMES = [
    "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller",
    "Davis", "Rodriguez", "Martinez", "Hernandez", "Lopez", "Wilson"
]

PROJECT_NAMES = [
    "Apollo", "Phoenix", "Atlas", "Titan", "Nexus", "Quantum", "Zenith",
    "Horizon", "Eclipse", "Vertex", "Catalyst", "Pinnacle"
]

POST_TITLES = [
    "Introduction to Machine Learning",
    "Best Practices in Software Development",
    "Understanding Distributed Systems",
    "The Future of Web Development",
    "Design Patterns Explained",
    "Database Optimization Techniques",
    "Microservices Architecture Guide",
    "Cloud Computing Essentials",
    "API Design Best Practices",
    "Testing Strategies for Modern Apps"
]

FILE_NAMES = [
    "screenshot.png", "diagram.jpg", "presentation.pdf", "code_sample.txt",
    "architecture.svg", "mockup.png", "flowchart.jpg", "report.pdf"
]

LANDMARK_NAMES = [
    "Eiffel Tower", "Statue of Liberty", "Big Ben", "Sydney Opera House",
    "Colosseum", "Taj Mahal", "Great Wall", "Machu Picchu",
    "Christ the Redeemer", "Petra", "Golden Gate Bridge", "Burj Khalifa"
]

LANDMARK_TAGS = [
    "Historic", "Modern", "Architecture", "UNESCO", "Monument",
    "Bridge", "Building", "Natural", "Wonder", "Cultural"
]

BIOS = [
    "Passionate about technology and innovation",
    "Full-stack developer with 5+ years experience",
    "Data scientist specializing in ML/AI",
    "Product manager focused on user experience",
    "DevOps engineer automating everything",
    "Security researcher and ethical hacker"
]

ADMIN_SIGNS = [
    "Project Lead", "Tech Lead", "Senior Developer", "Team Lead",
    "Principal Engineer", "Chief Architect"
]


class MarciDBClient:
    """Client for interacting with MarciDB HTTP API"""

    def __init__(self, base_url: str = BASE_URL):
        self.base_url = base_url
        self.created_ids = {
            "User": [],
            "TestUser": [],
            "TestProject": [],
            "Post": [],
            "Project": [],
            "File": [],
            "LandmarkTag": [],
            "Landmark": []
        }

    def insert(self, model: str, data: Dict[str, Any]) -> Dict[str, Any]:
        """Insert a document into the database"""
        url = f"{self.base_url}/{model}/insert"
        response = requests.post(url, json=data)

        if response.status_code != 200:
            print(f"Error inserting {model}: {response.status_code} - {response.text}")
            return None

        result = response.json()
        print(f"✓ Created {model}: {json.dumps(result, indent=2)}")

        # Store the ID for later reference
        self.created_ids[model].append(result)
        return result

    def find_many(self, model: str, select: Dict[str, Any] = None) -> List[Dict[str, Any]]:
        """Query documents from the database"""
        url = f"{self.base_url}/{model}/findMany"
        response = requests.post(url, json=select or {})

        if response.status_code != 200:
            print(f"Error querying {model}: {response.status_code}")
            return []

        return response.json()


def generate_random_email(first_name: str, last_name: str) -> str:
    """Generate a random email address"""
    domains = ["example.com", "test.com", "demo.org", "sample.net"]
    return f"{first_name.lower()}.{last_name.lower()}@{random.choice(domains)}"


def generate_random_datetime(days_ago_max: int = 365) -> int:
    """Generate a random datetime as epoch timestamp"""
    days_ago = random.randint(0, days_ago_max)
    hours_ago = random.randint(0, 23)
    dt = datetime.now() - timedelta(days=days_ago, hours=hours_ago)
    return int(dt.timestamp())


def generate_random_vector(size: int, normalized: bool = False) -> List[float]:
    """Generate a random vector"""
    vector = [random.uniform(-1.0, 1.0) for _ in range(size)]

    if normalized:
        # Normalize for cosine similarity
        magnitude = sum(x * x for x in vector) ** 0.5
        if magnitude > 0:
            vector = [x / magnitude for x in vector]

    return vector


def populate_files(client: MarciDBClient, count: int = 10):
    """Create File objects"""
    print("\n=== Creating Files ===")
    for i in range(count):
        file_data = {
            "name": random.choice(FILE_NAMES)
        }
        client.insert("File", file_data)


def populate_landmark_tags(client: MarciDBClient):
    """Create LandmarkTag objects"""
    print("\n=== Creating Landmark Tags ===")
    for tag in LANDMARK_TAGS:
        tag_data = {
            "name": tag
        }
        client.insert("LandmarkTag", tag_data)


def populate_landmarks(client: MarciDBClient, count: int = 12):
    """Create Landmark objects with vector embeddings and tags"""
    print("\n=== Creating Landmarks ===")

    # Ensure we have tags
    if not client.created_ids["LandmarkTag"]:
        populate_landmark_tags(client)

    for i in range(min(count, len(LANDMARK_NAMES))):
        # Random location (latitude, longitude approximation)
        location = [
            random.uniform(-90.0, 90.0),  # latitude
            random.uniform(-180.0, 180.0)  # longitude
        ]

        # Random embedding (2560 dimensions, normalized for cosine)
        embeddings = generate_random_vector(2560, normalized=True)

        # Assign 2-4 random tags
        num_tags = random.randint(2, 4)
        tag_ids = random.sample(client.created_ids["LandmarkTag"],
                                min(num_tags, len(client.created_ids["LandmarkTag"])))

        landmark_data = {
            "name": LANDMARK_NAMES[i],
            "description": f"A famous landmark known for its {random.choice(['architecture', 'history', 'beauty', 'cultural significance'])}",
            "location": location,
            "embeddings": embeddings,
            "tags": tag_ids
        }
        client.insert("Landmark", landmark_data)


def populate_users(client: MarciDBClient, count: int = 10):
    """Create User objects"""
    print("\n=== Creating Users ===")
    used_emails = set()

    for i in range(count):
        first_name = random.choice(FIRST_NAMES)
        last_name = random.choice(LAST_NAMES)

        # Ensure unique email
        email = generate_random_email(first_name, last_name)
        counter = 1
        while email in used_emails:
            email = f"{first_name.lower()}.{last_name.lower()}{counter}@example.com"
            counter += 1
        used_emails.add(email)

        user_data = {
            "name": first_name,
            "surname": last_name,
            "email": email,
            "info": {
                "bio": random.choice(BIOS)
            }
        }
        client.insert("User", user_data)


def populate_posts(client: MarciDBClient, count: int = 15):
    """Create Post objects with authors and images"""
    print("\n=== Creating Posts ===")

    # Ensure we have users and files
    if not client.created_ids["User"]:
        populate_users(client)
    if not client.created_ids["File"]:
        populate_files(client)

    for i in range(count):
        # Assign random author
        author_id = random.choice(client.created_ids["User"])

        # Assign 0-3 random images
        num_images = random.randint(0, 3)
        image_ids = random.sample(client.created_ids["File"],
                                  min(num_images, len(client.created_ids["File"])))

        post_data = {
            "title": random.choice(POST_TITLES),
            "createdAt": generate_random_datetime(days_ago_max=180),
            "author": author_id,
            "images": image_ids
        }
        client.insert("Post", post_data)


def populate_projects(client: MarciDBClient, count: int = 8):
    """Create Project objects with UserRole struct containing users and roles"""
    print("\n=== Creating Projects ===")

    # Ensure we have users
    if not client.created_ids["User"]:
        populate_users(client)

    for i in range(count):
        project_name = PROJECT_NAMES[i] if i < len(PROJECT_NAMES) else f"Project-{i + 1}"

        # Assign 2-5 random users to the project with roles
        num_users = random.randint(2, min(5, len(client.created_ids["User"])))
        selected_users = random.sample(client.created_ids["User"], num_users)

        users_roles = []
        for idx, user_id in enumerate(selected_users):
            # First user is creator, others might be admin or creator
            if idx == 0:
                # First user is always creator
                users_roles.append({
                    "user": user_id,
                    "role": "creator"
                })
            else:
                # 50% chance to be admin
                if random.random() < 0.5:
                    users_roles.append({
                        "user": user_id,
                        "role": "admin",
                        "sign": random.choice(ADMIN_SIGNS)
                    })
                else:
                    users_roles.append({
                        "user": user_id,
                        "role": "creator"
                    })

        project_data = {
            "name": project_name,
            "users": users_roles
        }
        client.insert("Project", project_data)


def populate_test_users(client: MarciDBClient, count: int = 6):
    """Create TestUser objects"""
    print("\n=== Creating Test Users ===")

    for i in range(count):
        test_user_data = {
            "name": f"TestUser-{i + 1}"
        }
        client.insert("TestUser", test_user_data)


def populate_test_projects(client: MarciDBClient, count: int = 5):
    """Create TestProject objects with multiple TestUsers"""
    print("\n=== Creating Test Projects ===")

    # Ensure we have test users
    if not client.created_ids["TestUser"]:
        populate_test_users(client)

    for i in range(count):
        # Assign 2-4 random test users to the project
        num_users = random.randint(2, min(4, len(client.created_ids["TestUser"])))
        selected_users = random.sample(client.created_ids["TestUser"], num_users)

        test_project_data = {
            "name": f"TestProject-{i + 1}",
            "users": selected_users
        }
        client.insert("TestProject", test_project_data)


def main():
    """Main function to populate the database"""
    print("=" * 60)
    print("MarciDB Population Script")
    print("=" * 60)

    client = MarciDBClient()

    try:
        # Populate in order to respect dependencies
        populate_files(client, count=10)
        populate_landmark_tags(client)
        populate_landmarks(client, count=12)
        populate_users(client, count=10)
        populate_posts(client, count=15)
        populate_projects(client, count=8)
        populate_test_users(client, count=6)
        populate_test_projects(client, count=5)

        print("\n" + "=" * 60)
        print("✓ Database population completed successfully!")
        print("=" * 60)
        print("\nSummary:")
        for model, ids in client.created_ids.items():
            if ids:
                print(f"  {model}: {len(ids)} objects created")

    except Exception as e:
        print(f"\n✗ Error during population: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()