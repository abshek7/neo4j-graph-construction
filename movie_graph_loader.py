from neo4j import GraphDatabase
import pandas as pd
import json
from typing import Dict, List
import logging
import os
import time

class MovieGraphLoader:
    def __init__(self, uri: str, username: str, password: str):
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)
        self.uri = uri
        self.username = username
        self.password = password
        
        if not self.logger.handlers:
            handler = logging.StreamHandler()
            handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
            self.logger.addHandler(handler)
        
        self.logger.info(f"Attempting to connect to Neo4j at {uri}")

        try:
            self.driver = GraphDatabase.driver(uri, auth=(username, password))
            with self.driver.session() as session:
                session.run("RETURN 1").single()
                self.logger.info("Successfully connected to Neo4j AuraDB")
        except Exception as e:
            self.logger.error(f"Failed to connect: {str(e)}")
            raise

    def close(self):
        self.driver.close()

    def load_movies(self, csv_path: str):
        if not os.path.exists(csv_path):
            self.logger.error(f"CSV file not found: {csv_path}")
            raise FileNotFoundError(f"CSV file not found: {csv_path}")

     
        self.logger.info(f"Reading CSV file: {csv_path}")
        df = pd.read_csv(csv_path)
        self.logger.info(f"Found {len(df)} movies in CSV")

     
        with self.driver.session() as session:
            session.run("CREATE CONSTRAINT movie_id IF NOT EXISTS FOR (m:Movie) REQUIRE m.id IS UNIQUE")
            session.run("CREATE INDEX movie_title IF NOT EXISTS FOR (m:Movie) ON (m.title)")

        
        batch_size = 40000  
        total_processed = 0
        
        for i in range(0, len(df), batch_size):
            try:
                batch = df.iloc[i:i + batch_size]
                self._process_movie_batch(batch)
                total_processed += len(batch)
                self.logger.info(f"Processed {total_processed}/{len(df)} movies")
                
                # Add a delay between batches to prevent overload
                time.sleep(0.5)
                
                # Reconnect every few batches to prevent timeout
                if total_processed % 50000 == 0:
                    self._ensure_connection()
                
            except Exception as e:
                self.logger.error(f"Error processing batch {i}-{i+batch_size}: {str(e)}")
                # Try to reconnect
                self._ensure_connection()
                # Retry the failed batch
                try:
                    self._process_movie_batch(batch)
                except Exception as retry_e:
                    self.logger.error(f"Retry failed: {str(retry_e)}")
                    continue
        
        self.logger.info("Finished loading movies")

    def _process_movie_batch(self, batch):
        # Clean and prepare the data
        cleaned_batch = []
        for record in batch.to_dict('records'):
            # Convert NaN to None/null for Neo4j
            clean_record = {}
            for key, value in record.items():
                if pd.isna(value):
                    clean_record[key] = None
                else:
                    clean_record[key] = value
            
            # Ensure genres is a list and remove any NaN values
            if 'genres' in clean_record:
                try:
                    if isinstance(clean_record['genres'], str):
                        # Try to safely parse the string as JSON
                        import json
                        try:
                            genres = json.loads(clean_record['genres'].replace("'", '"')) if clean_record['genres'] else []
                        except json.JSONDecodeError:
                            # If JSON parsing fails, split by comma
                            genres = [g.strip() for g in clean_record['genres'].split(',')] if clean_record['genres'] else []
                    else:
                        genres = clean_record['genres'] if clean_record['genres'] else []
                    
                    # Filter out NaN values and empty strings
                    clean_record['genres'] = [g for g in genres if g and not pd.isna(g) and str(g).strip()]
                except Exception as e:
                    self.logger.warning(f"Error processing genres for movie {clean_record.get('id', 'unknown')}: {str(e)}")
                    clean_record['genres'] = []
            
            cleaned_batch.append(clean_record)

        query = """
        UNWIND $movies as movie
        MERGE (m:Movie {id: movie.id})
        SET 
            m.title = movie.title,
            m.overview = movie.overview,
            m.release_date = movie.release_date,
            m.vote_average = CASE 
                WHEN movie.vote_average IS NOT NULL THEN movie.vote_average 
                ELSE 0.0 
            END,
            m.vote_count = CASE 
                WHEN movie.vote_count IS NOT NULL THEN movie.vote_count 
                ELSE 0 
            END,
            m.popularity = CASE 
                WHEN movie.popularity IS NOT NULL THEN movie.popularity 
                ELSE 0.0 
            END
        
        WITH m, movie
        UNWIND movie.genres as genre
        WITH m, genre
        WHERE genre IS NOT NULL
        MERGE (g:Genre {name: genre})
        MERGE (m)-[:HAS_GENRE]->(g)
        """

        with self.driver.session() as session:
            session.run(query, movies=cleaned_batch)

    def _ensure_connection(self):
        """Ensure connection is alive, reconnect if needed"""
        try:
            with self.driver.session() as session:
                session.run("RETURN 1").single()
        except Exception:
            self.logger.info("Reconnecting to Neo4j...")
            self.driver.close()
            time.sleep(1)  # Wait before reconnecting
            self.driver = GraphDatabase.driver(
                self.uri,
                auth=(self.username, self.password)
            ) 