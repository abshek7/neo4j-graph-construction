import os
from movie_graph_loader import MovieGraphLoader
from movie_query_interface import MovieQueryInterface
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def main():
    # Neo4j credentials
    NEO4J_URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
    NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
    NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD")
    
    # Verify that required environment variables are set
    if not all([NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD]):
        print("Error: Missing required Neo4j environment variables.")
        print(f"URI: {'Set' if NEO4J_URI else 'Missing'}")
        print(f"User: {'Set' if NEO4J_USER else 'Missing'}")
        print(f"Password: {'Set' if NEO4J_PASSWORD else 'Missing'}")
        return

    try:
        # Gemini API key
        GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")

        # Load data into Neo4j
        loader = MovieGraphLoader(NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD)
        loader.load_movies("tmdb_movies_2023.csv")
        loader.close()

        # Initialize query interface
        query_interface = MovieQueryInterface(
            NEO4J_URI,
            NEO4J_USER,
            NEO4J_PASSWORD,
            GEMINI_API_KEY
        )

        # Get general movie insights
        print("\n=== Movie Database Insights ===")
        insights = query_interface.get_movie_insights()
        for question, answer in insights.items():
            print(f"\nQ: {question}")
            print(f"A: {answer}")

        # Get personalized recommendations
        print("\n=== Movie Recommendations ===")
        movie = "The Dark Knight"
        recommendations = query_interface.get_personalized_recommendations(movie)
        print(f"\nRecommendations based on '{movie}':")
        print(recommendations)

        # Analyze genre trends
        print("\n=== Genre Trends Analysis ===")
        trends = query_interface.analyze_genre_trends(2023)
        print("\nTrends for 2023:")
        print(trends)

    except Exception as e:
        print(f"Error: {str(e)}")
        print("\nTroubleshooting steps:")
        print("1. Verify that your Neo4j server is running")
        print("2. Check if your Neo4j credentials are correct")
        print("3. Ensure your Neo4j URI is correct (e.g., 'neo4j+s://<YOUR-URI>' for AuraDB)")
        print("4. Verify that your IP is allowed to connect to the Neo4j instance")

if __name__ == "__main__":
    main() 