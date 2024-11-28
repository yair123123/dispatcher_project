from neo4j import GraphDatabase
from app.settings.config import *

driver = GraphDatabase.driver(
    NEO4J_URI,
    auth=(NEO4J_USER, NEO4J_PASSWORD)
)
