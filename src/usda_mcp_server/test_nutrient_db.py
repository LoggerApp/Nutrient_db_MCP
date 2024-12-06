import sqlite3
import logging
import pandas as pd
from tabulate import tabulate

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class NutrientDBTester:
    def __init__(self, db_path):
        self.db_path = db_path
        self.conn = None
        self.cursor = None
        
    def connect(self):
        try:
            self.conn = sqlite3.connect(self.db_path)
            self.cursor = self.conn.cursor()
            logger.info(f"Connected to database at {self.db_path}")
            return True
        except Exception as e:
            logger.error(f"Failed to connect to database: {str(e)}")
            return False
            
    def run_tests(self):
        """Run a series of tests to verify database integrity"""
        try:
            # Test 1: Check tables exist
            logger.info("\n=== Testing Table Structure ===")
            self.cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
            tables = self.cursor.fetchall()
            logger.info("Found tables:")
            for table in tables:
                logger.info(f"  - {table[0]}")
                
            # Test 2: Check record counts
            logger.info("\n=== Testing Record Counts ===")
            count_queries = {
                "foods": "SELECT COUNT(*) FROM foods",
                "food_nutrients": "SELECT COUNT(*) FROM food_nutrients",
                "nutrients": "SELECT COUNT(*) FROM nutrients",
                "common_portions": "SELECT COUNT(*) FROM common_portions"
            }
            
            for table, query in count_queries.items():
                self.cursor.execute(query)
                count = self.cursor.fetchone()[0]
                logger.info(f"{table}: {count:,} records")
                
            # Test 3: Sample data from each main table
            logger.info("\n=== Sampling Data ===")
            sample_queries = {
                "foods": "SELECT * FROM foods LIMIT 5",
                "food_nutrients": "SELECT * FROM food_nutrients LIMIT 5",
                "common_portions": "SELECT * FROM common_portions LIMIT 5"
            }
            
            for table, query in sample_queries.items():
                logger.info(f"\nSample from {table}:")
                self.cursor.execute(query)
                columns = [description[0] for description in self.cursor.description]
                rows = self.cursor.fetchall()
                print(tabulate(rows, headers=columns, tablefmt='grid'))
                
            # Test 4: Check relationships
            logger.info("\n=== Testing Relationships ===")
            # Check if all food_nutrients reference valid foods
            self.cursor.execute("""
                SELECT COUNT(*) FROM food_nutrients fn 
                LEFT JOIN foods f ON fn.food_id = f.id 
                WHERE f.id IS NULL
            """)
            orphaned = self.cursor.fetchone()[0]
            logger.info(f"Orphaned nutrient records: {orphaned:,}")
            
            # Test 5: Check nutrient distribution
            logger.info("\n=== Testing Nutrient Distribution ===")
            self.cursor.execute("""
                SELECT 
                    nutrient_id,
                    COUNT(*) as count,
                    AVG(amount) as avg_amount,
                    MIN(amount) as min_amount,
                    MAX(amount) as max_amount
                FROM food_nutrients
                GROUP BY nutrient_id
                LIMIT 10
            """)
            nutrient_stats = self.cursor.fetchall()
            print(tabulate(nutrient_stats, 
                         headers=['Nutrient ID', 'Count', 'Avg Amount', 'Min Amount', 'Max Amount'], 
                         tablefmt='grid'))
            
            logger.info("\nAll tests completed successfully!")
            
        except Exception as e:
            logger.error(f"Test failed: {str(e)}")
            raise
        finally:
            self.conn.close()

def main():
    db_path = "/Users/Shared/Documents/gpt pilot/gpt-pilot/workspace/food_db_analysis/optimized_nutrients.db"
    
    tester = NutrientDBTester(db_path)
    if tester.connect():
        tester.run_tests()
    else:
        logger.error("Failed to connect to database")

if __name__ == "__main__":
    main()