import sqlite3
import polars as pl
import os
from pathlib import Path
from typing import Dict, Any, Optional
import logging
from tqdm import tqdm
import time
import json
import re

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class OptimizedNutrientDB:
    def __init__(self, db_path: str, csv_dir: str):
        self.db_path = db_path
        self.csv_dir = csv_dir
        self.conn = None
        self.cursor = None
        self.category_name_to_id = {}
        self.valid_category_ids = set()
        self.default_category_id = 1  # Default to first category if no match found
        
        # Key nutrients we want to track and their categories
        self.nutrient_categories = {
            "macronutrients": {
                2047: {"name": "Energy (Atwater General Factors)", "unit": "KCAL"},
                2048: {"name": "Energy (Atwater Specific Factors)", "unit": "KCAL"},
                1003: {"name": "Protein", "unit": "G"},
                1004: {"name": "Total lipid (fat)", "unit": "G"},
                1005: {"name": "Carbohydrate, by difference", "unit": "G"},
                1079: {"name": "Fiber, total dietary", "unit": "G"},
                2000: {"name": "Sugars, total", "unit": "G"}
            },
            "minerals": {
                1087: {"name": "Calcium, Ca", "unit": "MG"},
                1089: {"name": "Iron, Fe", "unit": "MG"},
                1090: {"name": "Magnesium, Mg", "unit": "MG"},
                1091: {"name": "Phosphorus, P", "unit": "MG"},
                1092: {"name": "Potassium, K", "unit": "MG"},
                1093: {"name": "Sodium, Na", "unit": "MG"}
            },
            "vitamins": {
                1104: {"name": "Vitamin A, IU", "unit": "IU"},
                1106: {"name": "Vitamin A, RAE", "unit": "UG"},
                1109: {"name": "Vitamin E (alpha-tocopherol)", "unit": "MG"},
                1110: {"name": "Vitamin D (D2 + D3)", "unit": "IU"},
                1111: {"name": "Vitamin D2", "unit": "UG"}
            },
            "lipids": {
                1253: {"name": "Cholesterol", "unit": "MG"},
                1258: {"name": "Fatty acids, total saturated", "unit": "G"},
                1257: {"name": "Fatty acids, total trans", "unit": "G"}
            }
        }

        # Define RDA values and limits
        self.rda_values = {
            "Protein": {"rda": None, "limit": None, "is_ratio": True, "tdee_factor": 0.25},
            "Total lipid (fat)": {"rda": None, "limit": None, "is_ratio": True, "tdee_factor": 0.30},
            "Carbohydrate, by difference": {"rda": None, "limit": None, "is_ratio": True, "tdee_factor": 0.45},
            "Energy": {"rda": 2000, "limit": None, "is_ratio": False},
            "Water": {"rda": 2000, "limit": None, "is_ratio": False},
            "Caffeine": {"rda": None, "limit": 400, "is_ratio": False},
            "Sugars, total": {"rda": None, "limit": None, "is_ratio": True, "tdee_factor": 0.10},
            "Fiber, total dietary": {"rda": 28, "limit": None, "is_ratio": False},
            "Calcium, Ca": {"rda": 1000, "limit": 2500, "is_ratio": False},
            "Iron, Fe": {"rda": 8, "limit": 45, "is_ratio": False},
            "Magnesium, Mg": {"rda": 300, "limit": 700, "is_ratio": False},
            "Phosphorus, P": {"rda": 700, "limit": 4000, "is_ratio": False},
            "Potassium, K": {"rda": 1400, "limit": 6000, "is_ratio": False},
            "Sodium, Na": {"rda": 1000, "limit": 2300, "is_ratio": False},
            "Zinc, Zn": {"rda": 12, "limit": 100, "is_ratio": False},
            "Vitamin C, total ascorbic acid": {"rda": 90, "limit": 2000, "is_ratio": False},
            "Vitamin D": {"rda": 1000, "limit": 8000, "is_ratio": False},
            "Vitamin A, RAE": {"rda": 900, "limit": 3000, "is_ratio": False},
            "Vitamin E (alpha-tocopherol)": {"rda": 15, "limit": 1000, "is_ratio": False},
            "Thiamin": {"rda": 1.2, "limit": None, "is_ratio": False},
            "Riboflavin": {"rda": 1.3, "limit": None, "is_ratio": False},
            "Niacin": {"rda": 16, "limit": None, "is_ratio": False},
            "Vitamin B-6": {"rda": 1.3, "limit": 100, "is_ratio": False},
            "Folate, total": {"rda": 400, "limit": 1000, "is_ratio": False},
            "Vitamin B-12": {"rda": 2.4, "limit": None, "is_ratio": False},
            "Vitamin K (phylloquinone)": {"rda": 120, "limit": None, "is_ratio": False},
            "Cholesterol": {"rda": None, "limit": 300, "is_ratio": False},
            "Fatty acids, total saturated": {"rda": None, "limit": None, "is_ratio": True, "tdee_factor": 0.10},
            "Fatty acids, total monounsaturated": {"rda": None, "limit": None, "is_ratio": True, "tdee_factor": 0.15},
            "Fatty acids, total polyunsaturated": {"rda": None, "limit": None, "is_ratio": True, "tdee_factor": 0.10}
        }

        # Validation constants
        self.VALID_UNITS = {
            'g', 'mg', 'µg', 'IU', 'kcal', 'kJ', 'mL', 'L',
            'mcg', 'mg_ATE', 'mg_GAE', 'g_per_100g'
        }
        self.MIN_SERVING_SIZE = 0.1
        self.MAX_SERVING_SIZE = 5000  # grams

    def connect(self):
        """Establish database connection"""
        try:
            db_dir = os.path.dirname(self.db_path)
            if not os.path.exists(db_dir):
                os.makedirs(db_dir)
            
            self.conn = sqlite3.connect(self.db_path)
            self.cursor = self.conn.cursor()
            
            # Enable WAL mode for better concurrent access
            self.cursor.execute("PRAGMA journal_mode=WAL")
            # Enable foreign key constraints
            self.cursor.execute("PRAGMA foreign_keys=ON")
            # Optimize cache settings
            self.cursor.execute("PRAGMA cache_size=-2000000")  # 2GB cache
            self.cursor.execute("PRAGMA page_size=4096")
            self.cursor.execute("PRAGMA temp_store=MEMORY")
            
            logger.info(f"Connected to database at {self.db_path}")
        except Exception as e:
            logger.error(f"Failed to connect to database: {e}")
            raise

    def close(self):
        """Close database connection"""
        if self.conn:
            try:
                self.conn.commit()
                self.conn.close()
                logger.info("Database connection closed")
            except Exception as e:
                logger.error(f"Error closing database connection: {e}")

    def create_schema(self):
        """Create the database schema"""
        try:
            logger.info("Creating schema...")
            
            # Drop tables in correct order (child tables first)
            self.cursor.executescript("""
                -- First, drop dependent/child tables
                DROP TABLE IF EXISTS food_nutrients;
                DROP TABLE IF EXISTS nutrient_rankings;
                DROP TABLE IF EXISTS food_density_scores;
                DROP TABLE IF EXISTS common_portions;
                DROP TABLE IF EXISTS common_nutrients_mv;
                
                -- Then drop parent tables
                DROP TABLE IF EXISTS foods;
                DROP TABLE IF EXISTS nutrients;
                DROP TABLE IF EXISTS food_categories;
                
                -- Now create tables in reverse order (parent tables first)
                CREATE TABLE IF NOT EXISTS food_categories (
                    id INTEGER PRIMARY KEY,
                    name TEXT NOT NULL UNIQUE
                );
                
                CREATE TABLE IF NOT EXISTS nutrients (
                    id INTEGER PRIMARY KEY,
                    name TEXT NOT NULL,
                    unit TEXT NOT NULL,
                    nutrient_nbr TEXT,
                    rank REAL
                );
                
                CREATE TABLE IF NOT EXISTS foods (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    fdc_id INTEGER UNIQUE NOT NULL,
                    name TEXT NOT NULL,
                    category_id INTEGER,
                    base_serving_size REAL,
                    base_serving_unit TEXT,
                    data_quality_score REAL,
                    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (category_id) REFERENCES food_categories(id)
                );
                
                CREATE TABLE IF NOT EXISTS food_nutrients (
                    food_id INTEGER,
                    nutrient_id INTEGER,
                    amount REAL NOT NULL,
                    confidence_score REAL DEFAULT 1.0,
                    PRIMARY KEY (food_id, nutrient_id),
                    FOREIGN KEY (food_id) REFERENCES foods(id),
                    FOREIGN KEY (nutrient_id) REFERENCES nutrients(id)
                );
                
                CREATE TABLE IF NOT EXISTS nutrient_rankings (
                    food_id INTEGER,
                    nutrient_id INTEGER,
                    amount REAL,
                    percentile_rank REAL,
                    z_score REAL,
                    PRIMARY KEY (food_id, nutrient_id),
                    FOREIGN KEY (food_id) REFERENCES foods(id),
                    FOREIGN KEY (nutrient_id) REFERENCES nutrients(id)
                );
                
                CREATE TABLE IF NOT EXISTS food_density_scores (
                    food_id INTEGER PRIMARY KEY,
                    total_score REAL,
                    nutrient_completeness REAL,
                    calories_per_100g REAL,
                    category_scores TEXT,  -- JSON string
                    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (food_id) REFERENCES foods(id)
                );
                
                CREATE TABLE IF NOT EXISTS common_portions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    food_id INTEGER,
                    description TEXT,
                    gram_weight REAL,
                    household_measure TEXT,
                    FOREIGN KEY (food_id) REFERENCES foods(id)
                );
                
                CREATE TABLE IF NOT EXISTS common_nutrients_mv (
                    food_id INTEGER PRIMARY KEY,
                    name TEXT,
                    calories REAL,
                    protein REAL,
                    fat REAL,
                    carbohydrates REAL,
                    fiber REAL,
                    sugar REAL,
                    calcium REAL,
                    iron REAL,
                    vitamin_c REAL,
                    vitamin_d REAL,
                    vitamin_b12 REAL,
                    data_completeness REAL,
                    FOREIGN KEY (food_id) REFERENCES foods(id)
                );
                
                -- Create indexes for better query performance
                CREATE INDEX IF NOT EXISTS idx_foods_category ON foods(category_id);
                CREATE INDEX IF NOT EXISTS idx_food_nutrients_nutrient ON food_nutrients(nutrient_id);
                CREATE INDEX IF NOT EXISTS idx_food_nutrients_food ON food_nutrients(food_id);
                CREATE INDEX IF NOT EXISTS idx_foods_fdc ON foods(fdc_id);
            """)
            
            self.conn.commit()
            logger.info("Schema created successfully")
            
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Failed to create schema: {e}")
            raise

    def verify_data_integrity(self):
        """Verify data integrity and completeness"""
        try:
            logger.info("Verifying data integrity...")
            
            # Check core tables
            checks = [
                ("foods", "COUNT(*)", "total foods"),
                ("nutrients", "COUNT(*)", "total nutrients"),
                ("food_nutrients", "COUNT(*)", "total nutrient records"),
                ("nutrient_rankings", "COUNT(*)", "nutrient rankings"),
                ("food_density_scores", "COUNT(*)", "density scores")
            ]
            
            results = {}
            for table, query, description in checks:
                self.cursor.execute(f"SELECT {query} FROM {table}")
                count = self.cursor.fetchone()[0]
                results[description] = count
                
            # Check for orphaned records
            self.cursor.execute("""
                SELECT COUNT(*) FROM food_nutrients fn
                LEFT JOIN foods f ON fn.food_id = f.id
                WHERE f.id IS NULL
            """)
            orphaned = self.cursor.fetchone()[0]
            results["orphaned records"] = orphaned
            
            # Verify optimization coverage
            self.cursor.execute("""
                SELECT COUNT(*) FROM foods f
                LEFT JOIN food_density_scores fds ON f.id = fds.food_id
                WHERE fds.food_id IS NULL
            """)
            missing_scores = self.cursor.fetchone()[0]
            results["foods missing density scores"] = missing_scores
            
            # Log results
            logger.info("Data integrity verification results:")
            for desc, count in results.items():
                logger.info(f"- {desc}: {count}")
            
            return results
            
        except Exception as e:
            logger.error(f"Data integrity verification failed: {e}")
            raise

    def update_rankings_and_scores(self):
        """Update optimization tables with new data"""
        try:
            logger.info("Updating rankings and scores...")
            
            # Start transaction
            self.conn.execute("BEGIN")
            
            # Get timestamp for updates
            update_time = time.strftime('%Y-%m-%d %H:%M:%S')
            
            # Update nutrient rankings
            self.calculate_nutrient_rankings()
            
            # Update density scores
            self.calculate_density_scores()
            
            # Update common nutrients materialized view
            self.update_common_nutrients_mv()
            
            # Commit transaction
            self.conn.commit()
            
            # Verify updates
            self.verify_data_integrity()
            
            logger.info("Rankings and scores updated successfully")
            
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Failed to update rankings and scores: {e}")
            raise

    def export_nutrient_summaries(self, output_dir: Path):
        """Export nutrient summaries for analysis"""
        try:
            os.makedirs(output_dir, exist_ok=True)
            
            # Export top foods by nutrient
            self.cursor.execute("""
                SELECT 
                    n.name as nutrient_name,
                    n.category,
                    f.name as food_name,
                    nr.amount,
                    nr.density_score,
                    nr.amount_rank,
                    nr.density_rank
                FROM nutrient_rankings nr
                JOIN nutrients n ON nr.nutrient_id = n.id
                JOIN foods f ON nr.food_id = f.id
                WHERE nr.amount_rank <= 10 OR nr.density_rank <= 10
                ORDER BY n.name, nr.amount_rank, nr.density_rank
            """)
            
            with open(output_dir / 'top_foods_by_nutrient.json', 'w') as f:
                results = []
                current_nutrient = None
                nutrient_data = {}
                
                for row in self.cursor:
                    nutrient = row[0]
                    if nutrient != current_nutrient:
                        if nutrient_data:
                            results.append(nutrient_data)
                        current_nutrient = nutrient
                        nutrient_data = {
                            'nutrient': nutrient,
                            'category': row[1],
                            'top_by_amount': [],
                            'top_by_density': []
                        }
                    
                    food_data = {
                        'food': row[2],
                        'amount': row[3],
                        'density_score': row[4]
                    }
                    
                    if row[5] <= 10:  # amount_rank
                        nutrient_data['top_by_amount'].append(food_data)
                    if row[6] <= 10:  # density_rank
                        nutrient_data['top_by_density'].append(food_data)
                
                if nutrient_data:
                    results.append(nutrient_data)
                    
                json.dump(results, f, indent=2)
            
            # Export overall nutrient density leaders
            self.cursor.execute("""
                SELECT 
                    f.name,
                    fds.overall_density_score,
                    fds.protein_density,
                    fds.vitamin_density,
                    fds.mineral_density,
                    fds.fiber_density,
                    fds.nutrient_completeness,
                    fds.category_scores
                FROM food_density_scores fds
                JOIN foods f ON fds.food_id = f.id
                ORDER BY fds.overall_density_score DESC
                LIMIT 100
            """)
            
            with open(output_dir / 'top_nutrient_dense_foods.json', 'w') as f:
                results = [{
                    'food': row[0],
                    'overall_score': row[1],
                    'protein_density': row[2],
                    'vitamin_density': row[3],
                    'mineral_density': row[4],
                    'fiber_density': row[5],
                    'completeness': row[6],
                    'category_scores': json.loads(row[7])
                } for row in self.cursor]
                
                json.dump(results, f, indent=2)
            
            logger.info(f"Nutrient summaries exported to {output_dir}")
            
        except Exception as e:
            logger.error(f"Failed to export nutrient summaries: {e}")
            raise

    def create_maintenance_triggers(self):
        """Create triggers to maintain optimization tables"""
        try:
            # Trigger to update rankings when nutrient data changes
            self.cursor.execute("""
                CREATE TRIGGER IF NOT EXISTS update_rankings_on_nutrient_change
                AFTER INSERT OR UPDATE OR DELETE ON food_nutrients
                BEGIN
                    -- Mark affected rankings for update
                    INSERT OR REPLACE INTO nutrient_rankings (
                        nutrient_id, food_id, updated_at
                    )
                    VALUES (
                        COALESCE(NEW.nutrient_id, OLD.nutrient_id),
                        COALESCE(NEW.food_id, OLD.food_id),
                        CURRENT_TIMESTAMP
                    );
                END;
            """)
            
            # Trigger to update density scores when nutrient data changes
            self.cursor.execute("""
                CREATE TRIGGER IF NOT EXISTS update_density_on_nutrient_change
                AFTER INSERT OR UPDATE OR DELETE ON food_nutrients
                BEGIN
                    -- Mark affected scores for update
                    INSERT OR REPLACE INTO food_density_scores (
                        food_id, updated_at
                    )
                    VALUES (
                        COALESCE(NEW.food_id, OLD.food_id),
                        CURRENT_TIMESTAMP
                    );
                END;
            """)
            
            self.conn.commit()
            logger.info("Maintenance triggers created successfully")
            
        except Exception as e:
            logger.error(f"Failed to create maintenance triggers: {e}")
            raise

    def populate_nutrients(self):
        """Populate nutrients table with standardized names"""
        try:
            logger.info("Populating nutrients table...")
            
            # First, clear existing nutrients
            self.cursor.execute("DELETE FROM nutrients")
            
            # Read nutrient definitions from CSV
            nutrient_file = os.path.join(self.csv_dir, 'nutrient.csv')
            if not os.path.exists(nutrient_file):
                logger.error(f"Nutrient CSV file not found: {nutrient_file}")
                return
            
            nutrients_df = pl.read_csv(nutrient_file)
            logger.info(f"Found {len(nutrients_df)} nutrients in CSV file")
            logger.info(f"Nutrient CSV columns: {nutrients_df.columns}")
            
            # Insert nutrients from CSV
            for row in nutrients_df.iter_rows(named=True):
                try:
                    self.cursor.execute("""
                        INSERT INTO nutrients (
                            id,
                            name,
                            unit,
                            nutrient_nbr,
                            rank
                        ) VALUES (?, ?, ?, ?, ?)
                    """, (
                        int(row['id']),
                        row['name'],
                        row['unit_name'],
                        row['nutrient_nbr'],
                        float(row['rank']) if row['rank'] else None
                    ))
                except Exception as e:
                    logger.warning(f"Failed to insert nutrient {row['name']}: {e}")
                    logger.debug(f"Row data: {row}")
            
            # Verify nutrients were imported
            self.cursor.execute("SELECT COUNT(*) FROM nutrients")
            count = self.cursor.fetchone()[0]
            
            # Get sample of imported nutrients
            self.cursor.execute("""
                SELECT id, name, unit, nutrient_nbr, rank 
                FROM nutrients 
                ORDER BY rank 
                LIMIT 5
            """)
            samples = self.cursor.fetchall()
            
            self.conn.commit()
            logger.info(f"Nutrients table populated successfully with {count} nutrients")
            logger.info(f"Sample nutrients: {samples}")
            
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Failed to populate nutrients: {e}")
            raise

    def _build_category_mappings(self):
        """Build category name to ID mapping"""
        try:
            logger.info("Building category mappings...")
            
            # Get all categories from database
            self.cursor.execute("SELECT id, name FROM food_categories")
            categories = self.cursor.fetchall()
            
            # Build mappings
            self.category_name_to_id = {
                row[1].lower(): row[0] for row in categories
            }
            self.valid_category_ids = {row[0] for row in categories}
            
            logger.info(f"Built mappings for {len(self.category_name_to_id)} categories")
            
        except Exception as e:
            logger.error(f"Failed to build category mappings: {e}")
            raise

    def create_food_categories(self):
        try:
            logger.info("Creating food categories...")
            
            # Read categories from CSV
            categories_file = os.path.join(self.csv_dir, 'food_category.csv')
            categories_df = pl.read_csv(categories_file)
            
            # Log the columns we found
            logger.info(f"Food category CSV columns: {categories_df.columns}")
            
            # Insert categories
            for row in categories_df.iter_rows(named=True):
                try:
                    self.cursor.execute("""
                        INSERT OR REPLACE INTO food_categories (
                            id, 
                            name
                        ) VALUES (?, ?)
                    """, (
                        int(row['id']),
                        row['description']
                    ))
                except Exception as e:
                    logger.warning(f"Failed to insert category {row}: {e}")
            
            self.conn.commit()
            
            # Build category mappings after inserting categories
            self._build_category_mappings()
            
            # Verify categories were imported
            self.cursor.execute("SELECT COUNT(*) FROM food_categories")
            count = self.cursor.fetchone()[0]
            
            # Get sample of imported categories
            self.cursor.execute("SELECT id, name FROM food_categories LIMIT 5")
            samples = self.cursor.fetchall()
            
            logger.info(f"Food categories populated successfully with {count} categories")
            logger.info(f"Sample categories: {samples}")
            
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Failed to create food categories: {e}")
            raise

    def import_foundation_foods(self):
        try:
            logger.info("Importing foundation foods...")
            
            # First, clear all related tables to avoid foreign key issues
            logger.info("Clearing existing data...")
            self.cursor.execute("DELETE FROM food_nutrients")
            self.cursor.execute("DELETE FROM nutrient_rankings")
            self.cursor.execute("DELETE FROM food_density_scores")
            self.cursor.execute("DELETE FROM common_nutrients_mv")
            self.cursor.execute("DELETE FROM foods")
            self.conn.commit()
            
            # Read food data
            food_file = os.path.join(self.csv_dir, 'food.csv')
            foods_df = pl.read_csv(food_file)
            
            # Enhanced category variations mapping
            category_variations = {
                'beverages': [
                    'drink', 'juice', 'soda', 'coffee', 'tea', 'beverage', 'smoothie', 
                    'lemonade', 'water', 'cola', 'energy drink'
                ],
                'dairy and egg products': [
                    'milk', 'cheese', 'yogurt', 'dairy', 'ice cream', 'frozen yogurt',
                    'cream', 'butter', 'egg', 'custard', 'pudding', 'whey'
                ],
                'snacks': [
                    'chips', 'crackers', 'popcorn', 'pretzels', 'snack', 'granola bar', 
                    'energy bar', 'trail mix', 'peanuts', 'seeds', 'nuts', 'crisps',
                    'tortilla chips', 'potato chips', 'corn chips', 'mixed nuts'
                ],
                'baked products': [
                    'bread', 'cake', 'cookie', 'pastry', 'baked', 'muffin', 'roll', 'bun',
                    'bagel', 'croissant', 'donut', 'pie', 'brownie', 'biscuit', 'scone'
                ],
                'cereal grains and pasta': [
                    'cereal', 'grain', 'pasta', 'rice', 'wheat', 'oat', 'quinoa', 'barley',
                    'noodle', 'macaroni', 'spaghetti', 'couscous', 'ramen'
                ],
                'fast foods': [
                    'restaurant', 'takeout', 'fast-food', 'drive-thru', 'burger', 'pizza',
                    'sandwich', 'fries', 'taco', 'burrito'
                ],
                'vegetables and vegetable products': [
                    'vegetable', 'veggies', 'veg', 'pickle', 'olive', 'pepper', 'relish',
                    'salad', 'lettuce', 'tomato', 'carrot', 'potato', 'onion', 'garlic',
                    'broccoli', 'spinach', 'cucumber', 'celery', 'mushroom'
                ],
                'fruits and fruit juices': [
                    'fruit', 'fruits', 'apple', 'orange', 'banana', 'berry', 'grape',
                    'citrus', 'peach', 'pear', 'plum', 'mango', 'pineapple', 'melon'
                ],
                'spices and herbs': [
                    'spice', 'herb', 'seasoning', 'salt', 'marinade', 'tenderizer',
                    'pepper', 'garlic', 'oregano', 'basil', 'thyme', 'cinnamon',
                    'nutmeg', 'paprika', 'cumin', 'curry'
                ],
                'soups, sauces, and gravies': [
                    'soup', 'sauce', 'gravy', 'dip', 'salsa', 'ketchup', 'mustard',
                    'bbq', 'mayonnaise', 'dressing', 'broth', 'stock', 'chowder',
                    'stew', 'bouillon'
                ],
                'sweets': [
                    'candy', 'chocolate', 'sweet', 'dessert', 'sugar', 'syrup',
                    'honey', 'jam', 'jelly', 'marshmallow', 'caramel', 'fudge',
                    'taffy', 'gummy', 'licorice', 'lollipop', 'toffee'
                ]
            }

            # Add common brand names with their categories
            brand_categories = {
                'snacks': [
                    'doritos', 'lays', 'pringles', 'cheetos', 'ritz', 'nabisco', 'sunchips',
                    'tostitos', 'fritos', 'planters', 'chex mix', 'combos'
                ],
                'beverages': [
                    'coca-cola', 'coke', 'pepsi', 'sprite', 'fanta', 'gatorade', 'snapple',
                    'mountain dew', 'dr pepper', '7up', 'schweppes', 'red bull', 'monster'
                ],
                'sweets': [
                    'hershey', 'mars', 'nestle', 'cadbury', 'twix', 'snickers', 'milky way',
                    'kitkat', 'reeses', 'butterfinger', 'skittles', 'starburst'
                ],
                'dairy and egg products': [
                    'dannon', 'yoplait', 'breyers', 'häagen', 'haagen', 'ben jerry',
                    'chobani', 'kraft', 'philadelphia', 'sargento', 'velveeta'
                ]
            }

            # Merge brand categories into variations
            for category, brands in brand_categories.items():
                if category in category_variations:
                    category_variations[category].extend(brands)

            # Create the enhanced mapping
            category_name_map = {
                self._standardize_category_name(name): id 
                for name, id in self.category_name_to_id.items()
            }

            # Add all variations to the mapping
            for base_category, variations in category_variations.items():
                base_id = category_name_map.get(self._standardize_category_name(base_category))
                if base_id:
                    for variation in variations:
                        category_name_map[self._standardize_category_name(variation)] = base_id

            # Enhanced category matching logic
            def find_category_id(food_name, category_name):
                # Try direct numeric ID
                if category_name.isdigit():
                    numeric_id = int(category_name)
                    if numeric_id in self.valid_category_ids:
                        return numeric_id, "direct_id"

                # Standardize names
                std_category = self._standardize_category_name(category_name)
                std_food_name = self._standardize_category_name(food_name)

                # Try exact category name match
                if std_category in category_name_map:
                    return category_name_map[std_category], "exact_match"

                # Try matching against food name
                food_words = set(std_food_name.split())
                for key_term, cat_id in category_name_map.items():
                    if key_term in std_food_name:
                        return cat_id, f"name_match:{key_term}"

                # Try matching multiple words
                for base_category, variations in category_variations.items():
                    base_id = category_name_map.get(self._standardize_category_name(base_category))
                    if base_id:
                        for variation in variations:
                            if all(word in food_words for word in variation.split()):
                                return base_id, f"multi_word_match:{variation}"

                return self.default_category_id, "default"

            # Import foods
            imported_count = 0
            default_category_count = 0
            unknown_categories = {}
            category_matches = {}
            
            for row in foods_df.iter_rows(named=True):
                try:
                    category_name = row['food_category_id'].strip() if row['food_category_id'] else ''
                    food_name = row['description']
                    fdc_id = row['fdc_id']

                    category_id, match_method = find_category_id(food_name, category_name)
                    
                    # Track category matching statistics
                    category_matches[match_method] = category_matches.get(match_method, 0) + 1
                    
                    # Insert the food record
                    self.cursor.execute("""
                        INSERT OR REPLACE INTO foods 
                        (fdc_id, name, category_id, base_serving_size, base_serving_unit, 
                        data_quality_score, last_updated)
                        VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                    """, (
                        fdc_id,
                        food_name,
                        category_id,
                        100.0,
                        'g',
                        1.0
                    ))
                    
                    imported_count += 1
                    if imported_count % 10000 == 0:
                        self.conn.commit()
                        logger.info(f"Imported {imported_count} foods...")
                    
                except Exception as e:
                    logger.error(f"Error importing food: {food_name} (FDC ID: {fdc_id}, Category: {category_name}) - {str(e)}")
                    raise
            
            # Log category matching statistics
            logger.info("\nCategory matching statistics:")
            for method, count in category_matches.items():
                logger.info(f"  {method}: {count} foods ({count/imported_count*100:.1f}%)")
            
            if unknown_categories:
                logger.info("\nTop 10 unknown categories:")
                sorted_unknown = sorted(unknown_categories.items(), key=lambda x: x[1], reverse=True)[:10]
                for cat, count in sorted_unknown:
                    logger.info(f"  {cat}: {count} foods")
            
            self.conn.commit()
            logger.info(f"Foods imported successfully: {imported_count} total, {default_category_count} in default category")
            
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Failed to import foundation foods: {e}")
            raise

    def _standardize_category_name(self, name):
        """Standardize category name for matching"""
        if not name:
            return ""
        # Convert to lowercase and remove special characters
        standardized = name.lower()
        standardized = re.sub(r'[^a-z0-9\s]', ' ', standardized)
        # Replace multiple spaces with single space and trim
        standardized = ' '.join(standardized.split())
        return standardized

    def import_nutrient_data(self):
        """Import nutrient data for foods with checkpoint support"""
        try:
            logger.info("Importing nutrient data...")
            
            # Read nutrient data
            nutrient_file = os.path.join(self.csv_dir, 'food_nutrient.csv')
            nutrients_df = pl.read_csv(nutrient_file)
            total_rows = len(nutrients_df)
            logger.info(f"Found {total_rows} nutrient records in CSV")
            
            # Get food IDs mapping once
            self.cursor.execute("SELECT id, fdc_id FROM foods")
            food_map = {row[1]: row[0] for row in self.cursor.fetchall()}
            logger.info(f"Loaded {len(food_map)} food mappings")
            
            # Create checkpoint table if it doesn't exist
            self.cursor.execute("""
                CREATE TABLE IF NOT EXISTS import_checkpoint (
                    id INTEGER PRIMARY KEY,
                    table_name TEXT,
                    last_processed INTEGER,
                    total_records INTEGER,
                    status TEXT,
                    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Check for existing checkpoint
            self.cursor.execute("""
                SELECT last_processed FROM import_checkpoint 
                WHERE table_name = 'food_nutrients' 
                AND status = 'in_progress'
                ORDER BY timestamp DESC LIMIT 1
            """)
            checkpoint = self.cursor.fetchone()
            start_position = checkpoint[0] if checkpoint else 0
            
            if start_position > 0:
                logger.info(f"Resuming from checkpoint at position {start_position}")
            else:
                logger.info("Starting new import")
                # Clear existing data only if starting fresh
                self.cursor.execute("DELETE FROM food_nutrients")
            
            # Create or recreate temporary table
            self.cursor.execute("DROP TABLE IF EXISTS temp_nutrients")
            self.cursor.execute("""
                CREATE TEMPORARY TABLE temp_nutrients (
                    fdc_id INTEGER,
                    nutrient_id INTEGER,
                    amount REAL,
                    PRIMARY KEY (fdc_id, nutrient_id)
                ) WITHOUT ROWID
            """)
            
            # If resuming, load existing data into temp table
            if start_position > 0:
                logger.info("Reloading previously processed data...")
                self.cursor.execute("""
                    INSERT INTO temp_nutrients (fdc_id, nutrient_id, amount)
                    SELECT f.fdc_id, fn.nutrient_id, fn.amount
                    FROM food_nutrients fn
                    JOIN foods f ON f.id = fn.food_id
                """)
            
            # Process in larger batches
            batch_size = 100000
            processed = start_position
            checkpoint_interval = 500000  # Save checkpoint every 500k records
            
            # First pass: Load into temp table
            logger.info("Phase 1: Loading into temporary table...")
            try:
                for i in range(start_position, total_rows, batch_size):
                    batch = nutrients_df.slice(i, min(i + batch_size, total_rows))
                    batch_values = [
                        (row['fdc_id'], row['nutrient_id'], float(row['amount']))
                        for row in batch.iter_rows(named=True)
                        if row['amount'] is not None
                    ]
                    
                    # Direct insert into temp table
                    self.cursor.executemany(
                        "INSERT OR REPLACE INTO temp_nutrients VALUES (?, ?, ?)",
                        batch_values
                    )
                    
                    processed = i + len(batch)
                    
                    # Update checkpoint every checkpoint_interval
                    if processed % checkpoint_interval < batch_size:
                        self.cursor.execute("""
                            INSERT INTO import_checkpoint (table_name, last_processed, total_records, status)
                            VALUES (?, ?, ?, ?)
                        """, ('food_nutrients', processed, total_rows, 'in_progress'))
                        
                        # Convert processed batch to final table
                        logger.info("Saving checkpoint - converting batch to final table...")
                        self.cursor.execute("""
                            INSERT OR REPLACE INTO food_nutrients (food_id, nutrient_id, amount, confidence_score)
                            SELECT f.id, t.nutrient_id, t.amount, 1.0
                            FROM temp_nutrients t
                            JOIN foods f ON f.fdc_id = t.fdc_id
                        """)
                        
                        # Clear temp table after converting
                        self.cursor.execute("DELETE FROM temp_nutrients")
                        
                        self.conn.commit()
                        logger.info(f"Checkpoint saved at position {processed}/{total_rows}")
                        logger.info(f"Progress: {(processed/total_rows)*100:.1f}%")
                        
                        # Get current counts
                        self.cursor.execute("SELECT COUNT(*) FROM food_nutrients")
                        current_count = self.cursor.fetchone()[0]
                        logger.info(f"Current unique pairs: {current_count}")
            
                # Final conversion of any remaining records
                logger.info("Converting final batch to permanent table...")
                self.cursor.execute("""
                    INSERT OR REPLACE INTO food_nutrients (food_id, nutrient_id, amount, confidence_score)
                    SELECT f.id, t.nutrient_id, t.amount, 1.0
                    FROM temp_nutrients t
                    JOIN foods f ON f.fdc_id = t.fdc_id
                """)
                
                # Mark import as complete
                self.cursor.execute("""
                    INSERT INTO import_checkpoint (table_name, last_processed, total_records, status)
                    VALUES (?, ?, ?, ?)
                """, ('food_nutrients', total_rows, total_rows, 'completed'))
                
                # Get final counts
                self.cursor.execute("SELECT COUNT(*) FROM food_nutrients")
                final_count = self.cursor.fetchone()[0]
                
                self.conn.commit()
                logger.info(f"Nutrient data import completed: {final_count} unique pairs imported")
                
            except Exception as e:
                self.conn.commit()  # Save progress even if there's an error
                logger.error(f"Import interrupted: {e}")
                raise
            
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Failed to import nutrient data: {e}")
            raise

    def analyze_db(self):
        """Run ANALYZE to update SQLite statistics"""
        try:
            self.cursor.execute("ANALYZE")
            self.conn.commit()
            logger.info("Database analysis completed successfully")
        except Exception as e:
            logger.error(f"Failed to analyze database: {e}")
            raise

    def import_portions(self):
        """Import food portion data"""
        try:
            logger.info("Importing food portions...")
            
            # Read portion data
            portion_file = os.path.join(self.csv_dir, 'food_portion.csv')
            portions_df = pl.read_csv(portion_file)
            
            # Clear existing portions
            self.cursor.execute("DELETE FROM common_portions")
            
            # Get food mappings
            self.cursor.execute("SELECT id, fdc_id FROM foods")
            food_map = {row[1]: row[0] for row in self.cursor.fetchall()}
            
            values = []
            skipped = 0
            for row in portions_df.iter_rows(named=True):
                food_id = food_map.get(row['fdc_id'])
                if food_id is None:
                    skipped += 1
                    continue
                    
                try:
                    # Handle None values with defaults
                    amount = float(row.get('amount') or 100)
                    gram_weight = float(row.get('gram_weight') or 0)
                    
                    values.append((
                        food_id,
                        row.get('portion_description', ''),
                        gram_weight,
                        row.get('modifier', '')
                    ))
                except (ValueError, TypeError):
                    skipped += 1
                    continue
            
            if values:
                self.cursor.executemany("""
                    INSERT INTO common_portions 
                    (food_id, description, gram_weight, household_measure)
                    VALUES (?, ?, ?, ?)
                """, values)
            
            self.conn.commit()
            logger.info(f"Portion data imported successfully: {len(values)} portions, {skipped} skipped")
            
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Failed to import food portions: {e}")
            raise

    def update_common_nutrients_mv(self):
        """Update materialized view of common nutrients"""
        try:
            logger.info("Updating common nutrients materialized view...")
            
            # Clear existing data
            self.cursor.execute("DELETE FROM common_nutrients_mv")
            
            # Insert updated data
            self.cursor.execute("""
                INSERT INTO common_nutrients_mv (
                    food_id, name, calories, protein, fat, carbohydrates,
                    fiber, sugar, calcium, iron, vitamin_c, vitamin_d,
                    vitamin_b12, data_completeness
                )
                SELECT 
                    f.id,
                    f.name,
                    COALESCE(MAX(CASE WHEN n.name LIKE '%Energy%' THEN fn.amount END), 0) as calories,
                    COALESCE(MAX(CASE WHEN n.name LIKE '%Protein%' THEN fn.amount END), 0) as protein,
                    COALESCE(MAX(CASE WHEN n.name LIKE '%Total lipid%' THEN fn.amount END), 0) as fat,
                    COALESCE(MAX(CASE WHEN n.name LIKE '%Carbohydrate%' THEN fn.amount END), 0) as carbohydrates,
                    COALESCE(MAX(CASE WHEN n.name LIKE '%Fiber%' THEN fn.amount END), 0) as fiber,
                    COALESCE(MAX(CASE WHEN n.name LIKE '%Sugars%' THEN fn.amount END), 0) as sugar,
                    COALESCE(MAX(CASE WHEN n.name LIKE '%Calcium%' THEN fn.amount END), 0) as calcium,
                    COALESCE(MAX(CASE WHEN n.name LIKE '%Iron%' THEN fn.amount END), 0) as iron,
                    COALESCE(MAX(CASE WHEN n.name LIKE '%Vitamin C%' THEN fn.amount END), 0) as vitamin_c,
                    COALESCE(MAX(CASE WHEN n.name LIKE '%Vitamin D%' THEN fn.amount END), 0) as vitamin_d,
                    COALESCE(MAX(CASE WHEN n.name LIKE '%Vitamin B-12%' THEN fn.amount END), 0) as vitamin_b12,
                    CASE 
                        WHEN COUNT(*) >= 10 THEN 1.0
                        ELSE COUNT(*) / 10.0
                    END as data_completeness
                FROM foods f
                LEFT JOIN food_nutrients fn ON f.id = fn.food_id
                LEFT JOIN nutrients n ON fn.nutrient_id = n.id
                GROUP BY f.id, f.name
            """)
            
            self.conn.commit()
            
            # Get statistics
            self.cursor.execute("SELECT COUNT(*) FROM common_nutrients_mv")
            count = self.cursor.fetchone()[0]
            logger.info(f"Common nutrients view updated successfully with {count} records")
            
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Failed to update common nutrients view: {e}")
            raise

    def calculate_nutrient_rankings(self):
        """Calculate nutrient rankings for all foods"""
        try:
            logger.info("Calculating nutrient rankings...")
            
            # Clear existing rankings
            self.cursor.execute("DELETE FROM nutrient_rankings")
            
            # First, get the total number of nutrients to verify data
            self.cursor.execute("SELECT COUNT(*) FROM food_nutrients")
            total_nutrients = self.cursor.fetchone()[0]
            logger.info(f"Total nutrient records to process: {total_nutrients}")
            
            # Calculate rankings for each nutrient using a simpler approach
            self.cursor.execute("""
                WITH nutrient_stats AS (
                    SELECT 
                        nutrient_id,
                        AVG(CAST(amount AS FLOAT)) as avg_amount,
                        MIN(amount) as min_amount,
                        MAX(amount) as max_amount
                    FROM food_nutrients
                    WHERE amount > 0
                    GROUP BY nutrient_id
                )
                INSERT INTO nutrient_rankings (
                    food_id, nutrient_id, amount, percentile_rank, z_score
                )
                SELECT 
                    fn.food_id,
                    fn.nutrient_id,
                    fn.amount,
                    (
                        RANK() OVER (
                            PARTITION BY fn.nutrient_id 
                            ORDER BY fn.amount
                        ) * 100.0 / 
                        COUNT(*) OVER (PARTITION BY fn.nutrient_id)
                    ) as percentile_rank,
                    CASE 
                        WHEN ns.max_amount = ns.min_amount THEN 0
                        ELSE (CAST(fn.amount AS FLOAT) - ns.avg_amount) / 
                             (ns.max_amount - ns.min_amount)
                    END as z_score
                FROM food_nutrients fn
                JOIN nutrient_stats ns ON fn.nutrient_id = ns.nutrient_id
                WHERE fn.amount > 0
            """)
            
            # Verify the results
            self.cursor.execute("SELECT COUNT(*) FROM nutrient_rankings")
            count = self.cursor.fetchone()[0]
            
            # Get some sample data to verify
            self.cursor.execute("""
                SELECT food_id, nutrient_id, amount, percentile_rank, z_score 
                FROM nutrient_rankings 
                LIMIT 5
            """)
            samples = self.cursor.fetchall()
            
            self.conn.commit()
            logger.info(f"Nutrient rankings calculated successfully with {count} records")
            logger.debug(f"Sample rankings: {samples}")
            
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Failed to calculate nutrient rankings: {e}")
            raise

    def calculate_density_scores(self):
        """Calculate nutrient density scores for all foods"""
        try:
            logger.info("Calculating food density scores...")
            
            # Clear existing scores
            self.cursor.execute("DELETE FROM food_density_scores")
            
            # Calculate density scores using common nutrients
            self.cursor.execute("""
                INSERT INTO food_density_scores (
                    food_id,
                    total_score,
                    nutrient_completeness,
                    calories_per_100g,
                    category_scores,
                    last_updated
                )
                SELECT 
                    cn.food_id,
                    (COALESCE(cn.protein, 0) / 50.0 +  -- Protein RDA is ~50g
                     COALESCE(cn.fiber, 0) / 25.0 +    -- Fiber RDA is ~25g
                     COALESCE(cn.vitamin_c, 0) / 90.0 + -- Vitamin C RDA is 90mg
                     COALESCE(cn.iron, 0) / 18.0 +     -- Iron RDA is 18mg
                     COALESCE(cn.calcium, 0) / 1000.0   -- Calcium RDA is 1000mg
                    ) / 5.0 * 100 as total_score,
                    (CASE WHEN cn.protein > 0 THEN 1 ELSE 0 END +
                     CASE WHEN cn.fiber > 0 THEN 1 ELSE 0 END +
                     CASE WHEN cn.vitamin_c > 0 THEN 1 ELSE 0 END +
                     CASE WHEN cn.iron > 0 THEN 1 ELSE 0 END +
                     CASE WHEN cn.calcium > 0 THEN 1 ELSE 0 END
                    ) / 5.0 as nutrient_completeness,
                    COALESCE(cn.calories, 0) as calories_per_100g,
                    json_object(
                        'protein', COALESCE(cn.protein, 0),
                        'fiber', COALESCE(cn.fiber, 0),
                        'vitamin_c', COALESCE(cn.vitamin_c, 0),
                        'iron', COALESCE(cn.iron, 0),
                        'calcium', COALESCE(cn.calcium, 0)
                    ) as category_scores,
                    CURRENT_TIMESTAMP
                FROM common_nutrients_mv cn
            """)
            
            # Get count of scores
            self.cursor.execute("SELECT COUNT(*) FROM food_density_scores")
            count = self.cursor.fetchone()[0]
            
            self.conn.commit()
            logger.info(f"Density scores calculated successfully with {count} records")
            
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Failed to calculate density scores: {e}")
            raise

def main():
    # Define absolute paths
    db_path = "/Users/Shared/Documents/gpt pilot/gpt-pilot/workspace/food_db_analysis/optimized_nutrients.db"
    csv_dir = "/Users/Shared/Documents/gpt pilot/gpt-pilot/workspace/food_db_analysis/src/usda_mcp_server/FoodData_Central_csv_2024-10-31"
    
    db = OptimizedNutrientDB(db_path, csv_dir)
    try:
        # Initialize database connection
        if not db.conn:
            logging.info("Initializing database connection...")
            db.connect()  # Make sure you have a connect() method
        if not db.conn:
            raise ConnectionError("Failed to establish database connection")
            
        cursor = db.conn.cursor()
        
        # Check what's already been completed
        cursor.execute("SELECT COUNT(*) FROM food_nutrients")
        nutrient_count = cursor.fetchone()[0]
        
        if nutrient_count == 0:
            logger.info("Importing nutrients...")
            db.import_nutrient_data()
        else:
            logger.info(f"Skipping nutrient import (found {nutrient_count} existing records)")
        
        # Continue with portions import
        logger.info("Importing food portions...")
        db.import_portions()
        
        logger.info("Database setup completed successfully!")
        
    except Exception as e:
        logger.error(f"Setup failed: {str(e)}", exc_info=True)
        raise
    finally:
        if db.conn:
            db.conn.close()

if __name__ == "__main__":
    main()