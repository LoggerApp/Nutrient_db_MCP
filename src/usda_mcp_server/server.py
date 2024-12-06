from mcp.server import Server, NotificationOptions
from mcp.server.models import InitializationOptions
import mcp.server.stdio
from mcp.types import TextContent, Resource, Tool
from dataclasses import dataclass
from typing import Dict, Any, List, Optional
import sqlite3
import asyncio
import json

@dataclass
class ToolParameter:
    name: str
    description: str
    required: bool = False
    type: str = "string"

class USDADatabase:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.conn = None
    
    def connect(self):
        if not self.conn:
            self.conn = sqlite3.connect(self.db_path, timeout=1200.0)  # 600 second timeout
            self.conn.row_factory = sqlite3.Row
            # Set pragmas for better performance with large DB
            self.conn.execute("PRAGMA journal_mode=WAL")
            self.conn.execute("PRAGMA synchronous=NORMAL")
            self.conn.execute("PRAGMA cache_size=10000")
            self.conn.execute("PRAGMA temp_store=MEMORY")
        return self.conn

    def close(self):
        if self.conn:
            self.conn.close()
            self.conn = None

class USDAServer:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.db = sqlite3.connect(db_path)
        self.server = mcp.server.Server()
        self.setup_resources()
        self.setup_tools()

    def setup_resources(self):
        @self.server.list_resources()
        async def handle_list_resources() -> List[Resource]:
            return [
                Resource(
                    name="foods",
                    description="Search and filter food database",
                    uri_template="usda://foods/list?limit={limit}&offset={offset}&category={category}&data_quality={data_quality}"
                ),
                Resource(
                    name="food_details",
                    description="Detailed information about a specific food",
                    uri_template="usda://foods/{food_id}"
                ),
                Resource(
                    name="nutrients",
                    description="List of all tracked nutrients with RDA values",
                    uri_template="usda://nutrients/list"
                ),
                Resource(
                    name="food_categories",
                    description="List of food categories",
                    uri_template="usda://categories/list"
                ),
                Resource(
                    name="common_portions",
                    description="Common portion sizes for a food",
                    uri_template="usda://foods/{food_id}/portions"
                ),
                Resource(
                    name="nutrient_dense_foods",
                    description="Foods ranked by nutrient density",
                    uri_template="usda://foods/nutrient_dense?nutrient={nutrient_id}&limit={limit}"
                )
            ]

        @self.server.read_resource()
        async def handle_read_resource(uri: str) -> List[TextContent]:
            conn = self.db.connect()
            cursor = conn.cursor()

            if uri.startswith("usda://foods/list"):
                # Parse query parameters
                params = {}
                if '?' in uri:
                    query_str = uri.split('?')[1]
                    params = dict(param.split('=') for param in query_str.split('&'))
                
                limit = int(params.get('limit', 50))
                offset = int(params.get('offset', 0))
                category = params.get('category')
                min_quality = float(params.get('data_quality', 0.7))
                
                query = """
                    SELECT f.id, f.name, f.category_id, f.data_quality_score,
                           c.name as category_name,
                           mv.calories, mv.protein, mv.carbohydrates, mv.fat,
                           ds.total_score as nutrient_density_score,
                           ds.nutrient_completeness
                    FROM foods f
                    LEFT JOIN food_categories c ON f.category_id = c.id
                    LEFT JOIN common_nutrients_mv mv ON f.id = mv.food_id
                    LEFT JOIN food_density_scores ds ON f.id = ds.food_id
                    WHERE f.data_quality_score >= ?
                """
                params = [min_quality]
                
                if category:
                    query += " AND f.category_id = ?"
                    params.append(category)
                
                query += " ORDER BY ds.total_score DESC LIMIT ? OFFSET ?"
                params.extend([limit, offset])
                
                cursor.execute(query, params)
                foods = [dict(row) for row in cursor.fetchall()]
                
                return [TextContent(
                    type="text",
                    text=json.dumps({
                        "foods": foods,
                        "pagination": {"limit": limit, "offset": offset}
                    })
                )]

            elif uri.startswith("usda://foods/") and "/portions" in uri:
                food_id = uri.split('/')[3]
                cursor.execute("""
                    SELECT cp.*, f.name as food_name
                    FROM common_portions cp
                    JOIN foods f ON cp.food_id = f.id
                    WHERE cp.food_id = ?
                    ORDER BY cp.gram_weight
                """, (food_id,))
                
                portions = [dict(row) for row in cursor.fetchall()]
                return [TextContent(
                    type="text",
                    text=json.dumps({"portions": portions})
                )]

            elif uri.startswith("usda://foods/nutrient_dense"):
                params = dict(param.split('=') for param in uri.split('?')[1].split('&'))
                nutrient_id = params.get('nutrient_id')
                limit = int(params.get('limit', 50))
                
                cursor.execute("""
                    SELECT f.id, f.name, f.category_id, 
                           fn.amount as nutrient_amount,
                           ds.total_score as density_score,
                           mv.calories
                    FROM foods f
                    JOIN food_nutrients fn ON f.id = fn.food_id
                    JOIN food_density_scores ds ON f.id = ds.food_id
                    JOIN common_nutrients_mv mv ON f.id = mv.food_id
                    WHERE fn.nutrient_id = ?
                    ORDER BY (fn.amount / NULLIF(mv.calories, 0)) DESC
                    LIMIT ?
                """, (nutrient_id, limit))
                
                foods = [dict(row) for row in cursor.fetchall()]
                return [TextContent(
                    type="text",
                    text=json.dumps({"foods": foods})
                )]

            elif uri.startswith("usda://foods/"):
                food_id = uri.split('/')[-1]
                cursor.execute("""
                    SELECT f.*, c.name as category_name,
                           mv.calories, mv.protein, mv.fat, mv.carbohydrates,
                           mv.fiber, mv.sugar, mv.calcium, mv.iron,
                           mv.vitamin_c, mv.vitamin_d, mv.vitamin_b12,
                           ds.total_score as nutrient_density_score,
                           ds.nutrient_completeness,
                           ds.category_scores as nutrient_category_scores
                    FROM foods f
                    LEFT JOIN food_categories c ON f.category_id = c.id
                    LEFT JOIN common_nutrients_mv mv ON f.id = mv.food_id
                    LEFT JOIN food_density_scores ds ON f.id = ds.food_id
                    WHERE f.id = ?
                """, (food_id,))
                
                food = dict(cursor.fetchone() or {})
                
                if food:
                    # Get detailed nutrient information
                    cursor.execute("""
                        SELECT n.id, n.name, n.unit, n.rda, n.upper_limit,
                               fn.amount, fn.confidence_score
                        FROM food_nutrients fn
                        JOIN nutrients n ON fn.nutrient_id = n.id
                        WHERE fn.food_id = ?
                    """, (food_id,))
                    
                    food['nutrients'] = [dict(row) for row in cursor.fetchall()]
                    
                    # Get portion sizes
                    cursor.execute("""
                        SELECT *
                        FROM common_portions
                        WHERE food_id = ?
                        ORDER BY gram_weight
                    """, (food_id,))
                    
                    food['portions'] = [dict(row) for row in cursor.fetchall()]
                
                return [TextContent(
                    type="text",
                    text=json.dumps(food)
                )]

            elif uri == "usda://nutrients/list":
                cursor.execute("""
                    SELECT n.*, 
                           COUNT(DISTINCT fn.food_id) as food_count,
                           AVG(fn.amount) as average_amount
                    FROM nutrients n
                    LEFT JOIN food_nutrients fn ON n.id = fn.nutrient_id
                    GROUP BY n.id
                """)
                nutrients = [dict(row) for row in cursor.fetchall()]
                return [TextContent(
                    type="text",
                    text=json.dumps({"nutrients": nutrients})
                )]

            elif uri == "usda://categories/list":
                cursor.execute("""
                    SELECT c.*, COUNT(f.id) as food_count
                    FROM food_categories c
                    LEFT JOIN foods f ON c.id = f.category_id
                    GROUP BY c.id
                """)
                categories = [dict(row) for row in cursor.fetchall()]
                return [TextContent(
                    type="text",
                    text=json.dumps({"categories": categories})
                )]

            return [TextContent(
                type="text",
                text=json.dumps({"error": "Resource not found"})
            )]

    def setup_tools(self):
        @self.server.list_tools()
        async def handle_list_tools() -> List[Tool]:
            return [
                Tool(
                    name="search_foods",
                    description="Search foods by name and nutrient content with quality filters",
                    parameters=[
                        ToolParameter(
                            name="query",
                            description="Search term for food names",
                            required=False,
                            type="string"
                        ),
                        ToolParameter(
                            name="nutrient_filters",
                            description="Filters for nutrient content",
                            required=False,
                            type="object"
                        ),
                        ToolParameter(
                            name="min_quality",
                            description="Minimum data quality score (0-1)",
                            required=False,
                            type="number"
                        ),
                        ToolParameter(
                            name="limit",
                            description="Maximum number of results",
                            required=False,
                            type="integer"
                        )
                    ]
                ),
                Tool(
                    name="analyze_nutrients",
                    description="Analyze nutrient content with confidence scores",
                    parameters=[
                        ToolParameter(
                            name="food_ids",
                            description="List of food IDs to analyze",
                            required=True,
                            type="array"
                        ),
                        ToolParameter(
                            name="nutrients",
                            description="List of nutrients to analyze",
                            required=False,
                            type="array"
                        ),
                        ToolParameter(
                            name="include_rda",
                            description="Include RDA percentages",
                            required=False,
                            type="boolean"
                        )
                    ]
                ),
                Tool(
                    name="find_nutrient_dense_foods",
                    description="Find foods with high nutrient density",
                    parameters=[
                        ToolParameter(
                            name="nutrient",
                            description="Target nutrient",
                            required=True,
                            type="string"
                        ),
                        ToolParameter(
                            name="min_density",
                            description="Minimum density ratio",
                            required=False,
                            type="number"
                        ),
                        ToolParameter(
                            name="limit",
                            description="Maximum number of results",
                            required=False,
                            type="integer"
                        )
                    ]
                )
            ]

        @self.server.call_tool()
        async def handle_call_tool(name: str, arguments: Dict[str, Any]) -> List[TextContent]:
            conn = self.db.connect()
            cursor = conn.cursor()

            if name == "search_foods":
                query = arguments.get("query", "").strip()
                nutrient_filters = arguments.get("nutrient_filters", {})
                min_quality = float(arguments.get("min_quality", 0.7))
                limit = int(arguments.get("limit", 10))

                sql = """
                    SELECT DISTINCT f.id, f.name, f.category_id, f.data_quality_score,
                           mv.calories, mv.protein, mv.carbohydrates, mv.fat
                    FROM foods f
                    JOIN common_nutrients_mv mv ON f.id = mv.food_id
                """
                
                conditions = ["f.data_quality_score >= ?"]
                args = [min_quality]

                if query:
                    conditions.append("f.name LIKE ?")
                    args.append(f"%{query}%")

                for nutrient, constraints in nutrient_filters.items():
                    join_idx = len(conditions)
                    sql += f"""
                        JOIN food_nutrients fn{join_idx} ON f.id = fn{join_idx}.food_id
                        JOIN nutrients n{join_idx} ON fn{join_idx}.nutrient_id = n{join_idx}.id
                    """
                    conditions.append(f"n{join_idx}.name = ?")
                    args.append(nutrient)

                    if "min" in constraints:
                        conditions.append(f"fn{join_idx}.amount >= ?")
                        args.append(constraints["min"])
                    if "max" in constraints:
                        conditions.append(f"fn{join_idx}.amount <= ?")
                        args.append(constraints["max"])

                if conditions:
                    sql += " WHERE " + " AND ".join(conditions)

                sql += " LIMIT ?"
                args.append(limit)

                cursor.execute(sql, args)
                results = [dict(row) for row in cursor.fetchall()]

                return [TextContent(
                    type="text",
                    text=json.dumps({
                        "results": results,
                        "nutrient": nutrient,
                        "density_metric": "amount per 1000 calories"
                    })
                )]

            return [TextContent(
                type="text",
                text=json.dumps({"error": "Tool not found"})
            )]

    async def run(self):
        async with mcp.server.stdio.stdio_server() as (read_stream, write_stream):
            await self.server.run(
                read_stream,
                write_stream,
                InitializationOptions(
                    server_name="usda-nutrition",
                    server_version="0.2.0",
                    request_timeout=1200,  # 600 seconds timeout
                    capabilities=self.server.get_capabilities(
                        notification_options=NotificationOptions(
                            timeout=1200  # 600 seconds timeout for notifications
                        ),
                        experimental_capabilities={}
                    )
                )
            )

def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--db-path', required=True, help='Path to USDA SQLite database')
    args = parser.parse_args()
    
    server = USDAServer(args.db_path)
    
    try:
        asyncio.run(server.run())
    finally:
        server.db.close()

if __name__ == "__main__":
    main()