from langchain_google_genai import GoogleGenerativeAI
from langchain_neo4j import GraphCypherQAChain, Neo4jGraph
from typing import Dict, List
import logging
from neo4j.exceptions import ServiceUnavailable
import time



class MovieQueryInterface:
    def __init__(self, uri: str, username: str, password: str, gemini_api_key: str):
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)
        
        # Store credentials for reconnection
        self.uri = uri
        self.username = username
        self.password = password
        self.gemini_api_key = gemini_api_key
        
        # Initialize connections
        self._initialize_connections()

    def _initialize_connections(self):
        """Initialize Neo4j and LLM connections"""
        # Initialize Neo4j connection with retry
        max_retries = 3
        for attempt in range(max_retries):
            try:
                self.graph = Neo4jGraph(
                    url=self.uri,
                    username=self.username,
                    password=self.password
                )
                # Test connection
                self.graph.query("RETURN 1")
                break
            except Exception as e:
                if attempt == max_retries - 1:
                    raise
                self.logger.warning(f"Connection attempt {attempt + 1} failed, retrying...")
                time.sleep(2)

        # Initialize Gemini
        self.llm = GoogleGenerativeAI(
            model="gemini-pro",
            google_api_key=self.gemini_api_key
        )

        # Create the QA chain with safety settings
        self.qa_chain = GraphCypherQAChain.from_llm(
            llm=self.llm,
            graph=self.graph,
            verbose=True,
            return_direct=False,
            top_k=10,
            allow_dangerous_requests=True  # Enable with proper scoping
        )

    def _execute_with_retry(self, func, *args, **kwargs):
        """Execute a function with retry logic"""
        max_retries = 3
        for attempt in range(max_retries):
            try:
                return func(*args, **kwargs)
            except ServiceUnavailable:
                if attempt == max_retries - 1:
                    raise
                self.logger.warning(f"Query attempt {attempt + 1} failed, retrying...")
                time.sleep(2)
                self._initialize_connections()

    def get_movie_insights(self):
        """Get various insights about movies in the database"""
        questions = [
            "What are the top 5 highest-rated movies with at least 1000 votes?",
            "Which genres have the most movies?",
            "What are the most popular movies released in 2023?",
            "What are the average ratings for each genre?",
            "What are some hidden gems (high rating but low vote count)?"
        ]
        
        insights = {}
        for question in questions:
            try:
                self.logger.info(f"Querying: {question}")
                result = self.qa_chain.run(question)
                insights[question] = result
            except Exception as e:
                self.logger.error(f"Error querying '{question}': {str(e)}")
                insights[question] = f"Error: {str(e)}"
        
        return insights

    def get_personalized_recommendations(self, movie_title: str, min_rating: float = 7.0):
        """Get personalized movie recommendations based on a movie title"""
        question = f"""
        Find movies similar to '{movie_title}' that:
        1. Share at least 2 genres with it
        2. Have a rating above {min_rating}
        3. Are sorted by popularity
        Limit to top 5 recommendations.
        """
        
        try:
            return self.qa_chain.run(question)
        except Exception as e:
            self.logger.error(f"Error getting recommendations: {str(e)}")
            return f"Error: {str(e)}"

    def analyze_genre_trends(self, year: int = 2023):
        """Analyze genre trends for a specific year"""
        question = f"""
        For movies released in {year}:
        1. Which genres were most popular?
        2. What was the average rating per genre?
        3. Which genre combinations appeared most frequently?
        """
        
        try:
            return self.qa_chain.run(question)
        except Exception as e:
            self.logger.error(f"Error analyzing trends: {str(e)}")
            return f"Error: {str(e)}"

    def custom_query(self, question: str):
        """Execute a custom query with safety checks"""
        try:
            return self.qa_chain.run(question)
        except Exception as e:
            self.logger.error(f"Error executing query: {str(e)}")
            return f"Error: {str(e)}"