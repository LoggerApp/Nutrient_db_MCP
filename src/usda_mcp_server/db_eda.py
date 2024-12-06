import pandas as pd
import os
from typing import Dict, Any
import json
import numpy as np
import re

def deep_convert_dict(obj):
    """Recursively convert all values and keys in nested dictionaries to JSON-serializable types."""
    if isinstance(obj, dict):
        return {str(k): deep_convert_dict(v) for k, v in obj.items()}
    elif isinstance(obj, (list, tuple)):
        return [deep_convert_dict(item) for item in obj]
    elif isinstance(obj, (np.integer, np.int64, pd.Int64Dtype)):
        return int(obj)
    elif isinstance(obj, (np.floating, np.float64)):
        return float(obj)
    elif isinstance(obj, np.ndarray):
        return obj.tolist()
    elif isinstance(obj, pd.Series):
        return deep_convert_dict(obj.to_dict())
    elif isinstance(obj, pd.DataFrame):
        return deep_convert_dict(obj.to_dict('records'))
    elif isinstance(obj, type):
        return str(obj)
    elif pd.isna(obj):
        return None
    return obj

def analyze_csv(filepath: str) -> Dict[str, Any]:
    """Analyze a CSV file and return its structure and statistics."""
    try:
        # Read the CSV file with low_memory=False to avoid DtypeWarning
        df = pd.read_csv(filepath, low_memory=False)
        
        analysis = {
            'filename': os.path.basename(filepath),
            'columns': list(df.columns),
            'total_rows': len(df),
            'data_types': {col: str(dtype) for col, dtype in df.dtypes.items()},
            'null_counts': df.isnull().sum().to_dict(),
            'null_percentages': (df.isnull().sum() / len(df) * 100).round(2).to_dict(),
            'unique_counts': {col: len(df[col].unique()) for col in df.columns},
            'sample_values': {col: df[col].dropna().head(5).tolist() for col in df.columns},
            'value_counts': {
                col: df[col].value_counts().head(5).to_dict()
                for col in df.columns 
                if df[col].dtype in ['object', 'int64'] and len(df[col].unique()) < 100
            }
        }
        
        # Add numeric column statistics
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        if len(numeric_cols) > 0:
            analysis['numeric_stats'] = {
                col: {
                    'min': df[col].min(),
                    'max': df[col].max(),
                    'mean': df[col].mean(),
                    'median': df[col].median(),
                    'std': df[col].std()
                } for col in numeric_cols
            }
        
        return deep_convert_dict(analysis)
        
    except Exception as e:
        return {
            'filename': os.path.basename(filepath),
            'error': str(e),
            'error_type': str(type(e))
        }

def analyze_nutrient_mappings(base_path: str) -> Dict[str, Any]:
    """Specifically analyze nutrient-related mappings and relationships."""
    try:
        # Load relevant files
        nutrient_df = pd.read_csv(os.path.join(base_path, 'nutrient.csv'), low_memory=False)
        food_nutrient_df = pd.read_csv(os.path.join(base_path, 'food_nutrient.csv'), low_memory=False)
        
        analysis = {
            'nutrient_counts': {
                'total_nutrients': len(nutrient_df),
                'total_food_nutrient_records': len(food_nutrient_df),
                'unique_nutrient_ids_in_mapping': len(food_nutrient_df['nutrient_id'].unique()),
            },
            'nutrient_details': {
                'all_nutrients': nutrient_df[['id', 'name', 'unit_name']].to_dict('records'),
                'nutrients_by_unit': nutrient_df.groupby('unit_name')['name'].count().to_dict(),
                'top_occurring_nutrients': food_nutrient_df['nutrient_id'].value_counts().head(20).to_dict()
            },
            'nutrient_id_ranges': {
                'nutrient_table': {
                    'min': nutrient_df['id'].min(),
                    'max': nutrient_df['id'].max(),
                },
                'food_nutrient_table': {
                    'min': food_nutrient_df['nutrient_id'].min(),
                    'max': food_nutrient_df['nutrient_id'].max(),
                }
            },
            'unmapped_nutrient_ids': {
                'in_mapping_not_in_nutrients': list(set(food_nutrient_df['nutrient_id'].unique()) - 
                                                  set(nutrient_df['id'].unique())),
                'in_nutrients_not_in_mapping': list(set(nutrient_df['id'].unique()) - 
                                                  set(food_nutrient_df['nutrient_id'].unique()))
            }
        }
        
        # Add nutrient value distribution analysis
        value_distributions = {}
        for nutrient_id in food_nutrient_df['nutrient_id'].unique():
            nutrient_name = nutrient_df[nutrient_df['id'] == nutrient_id]['name'].iloc[0] if len(nutrient_df[nutrient_df['id'] == nutrient_id]) > 0 else f"Unknown ({nutrient_id})"
            values = food_nutrient_df[food_nutrient_df['nutrient_id'] == nutrient_id]['amount']
            
            value_distributions[nutrient_id] = {
                'name': nutrient_name,
                'count': len(values),
                'non_zero_count': len(values[values > 0]),
                'min': values.min(),
                'max': values.max(),
                'mean': values.mean(),
                'median': values.median(),
                'std': values.std()
            }
        
        analysis['nutrient_value_distributions'] = value_distributions
        
        return deep_convert_dict(analysis)
        
    except Exception as e:
        return {
            'error': str(e),
            'error_type': str(type(e))
        }

def validate_data_integrity(base_path: str) -> Dict[str, Any]:
    """Validate data integrity across related tables."""
    try:
        # Load relevant files
        food_df = pd.read_csv(os.path.join(base_path, 'food.csv'), low_memory=False)
        food_nutrient_df = pd.read_csv(os.path.join(base_path, 'food_nutrient.csv'), low_memory=False)
        nutrient_df = pd.read_csv(os.path.join(base_path, 'nutrient.csv'), low_memory=False)
        
        validation = {
            'food_counts': {
                'total_foods': len(food_df),
                'unique_fdc_ids': len(food_df['fdc_id'].unique()),
                'foods_with_nutrients': len(food_nutrient_df['fdc_id'].unique()),
            },
            'nutrient_coverage': {
                'foods_missing_nutrients': len(set(food_df['fdc_id']) - 
                                            set(food_nutrient_df['fdc_id'])),
                'avg_nutrients_per_food': len(food_nutrient_df) / len(food_df['fdc_id'].unique())
            },
            'data_quality': {
                'null_fdc_ids': food_df['fdc_id'].isnull().sum(),
                'duplicate_fdc_ids': (food_df['fdc_id'].value_counts() > 1).sum(),
                'foods_without_category': food_df['food_category_id'].isnull().sum(),
            }
        }
        
        # Check for potential data type mismatches
        validation['type_validation'] = {
            'food_category_id_types': food_df['food_category_id'].apply(type).value_counts().to_dict(),
            'fdc_id_types': food_df['fdc_id'].apply(type).value_counts().to_dict(),
            'nutrient_id_types': nutrient_df['id'].apply(type).value_counts().to_dict()
        }
        
        return deep_convert_dict(validation)
        
    except Exception as e:
        return {
            'error': str(e),
            'error_type': str(type(e))
        }

def analyze_database_structure(base_path: str) -> Dict[str, Any]:
    """Analyze the structure of all CSV files in the database."""
    analysis_results = {}
    
    # List of expected CSV files
    expected_files = [
        'food.csv',
        'food_nutrient.csv',
        'nutrient.csv',
        'food_portion.csv',
        'food_category.csv'
    ]
    
    for filename in expected_files:
        filepath = os.path.join(base_path, filename)
        if os.path.exists(filepath):
            print(f"\nAnalyzing {filename}...")
            analysis_results[filename] = analyze_csv(filepath)
        else:
            print(f"Warning: {filename} not found in {base_path}")
            analysis_results[filename] = {
                'error': 'File not found',
                'filename': filename
            }
    
    return deep_convert_dict(analysis_results)

def generate_nutrient_mapping(base_path: str) -> Dict[str, Any]:
    """Generate a mapping of nutrient categories to USDA nutrient IDs."""
    try:
        # Load nutrient data
        nutrient_df = pd.read_csv(os.path.join(base_path, 'nutrient.csv'), low_memory=False)
        
        # Define search patterns for each category
        category_patterns = {
            'macronutrients': [
                r'protein|carbohydrate|fat|lipid|energy|calorie|sugar|fiber',
            ],
            'minerals': [
                r'calcium|iron|magnesium|phosphorus|potassium|sodium|zinc|iodine|selenium',
            ],
            'vitamins': [
                r'vitamin|thiamin|riboflavin|niacin|folate|biotin|choline',
            ],
            'lipids': [
                r'cholesterol|fatty acid|omega|saturated|monounsaturated|polyunsaturated',
            ]
        }
        
        # Create mapping dictionary
        nutrient_mapping = {category: [] for category in category_patterns.keys()}
        uncategorized = []
        
        # Categorize each nutrient
        for _, nutrient in nutrient_df.iterrows():
            categorized = False
            nutrient_info = {
                'id': int(nutrient['id']),
                'name': nutrient['name'],
                'unit': nutrient['unit_name']
            }
            
            for category, patterns in category_patterns.items():
                if any(bool(re.search(pattern, nutrient['name'], re.IGNORECASE)) 
                       for pattern in patterns):
                    nutrient_mapping[category].append(nutrient_info)
                    categorized = True
                    break
            
            if not categorized:
                uncategorized.append(nutrient_info)
        
        # Add uncategorized nutrients to the mapping
        nutrient_mapping['uncategorized'] = uncategorized
        
        # Generate Python code for the mapping
        code_template = """
# Updated nutrient categories mapping
nutrient_categories = {
    'macronutrients': {
        # Energy and Calories
        %(energy)s,
        # Protein
        %(protein)s,
        # Carbohydrates
        %(carbs)s,
        # Fats
        %(fats)s,
        # Fiber
        %(fiber)s,
        # Sugars
        %(sugars)s
    },
    'minerals': {
        # Essential minerals
        %(minerals)s
    },
    'vitamins': {
        # Fat-soluble vitamins
        %(fat_vitamins)s,
        # Water-soluble vitamins
        %(water_vitamins)s
    },
    'lipids': {
        # Cholesterol and fatty acids
        %(lipids)s
    }
}
"""
        
        return {
            'raw_mapping': nutrient_mapping,
            'stats': {
                'total_nutrients': len(nutrient_df),
                'categorized': sum(len(nutrients) for nutrients in nutrient_mapping.values()),
                'by_category': {
                    category: len(nutrients) 
                    for category, nutrients in nutrient_mapping.items()
                }
            }
        }
        
    except Exception as e:
        return {
            'error': str(e),
            'error_type': str(type(e))
        }

def main():
    # Base path to your CSV files
    base_path = "/Users/Shared/Documents/gpt pilot/gpt-pilot/workspace/food_db_analysis/src/usda_mcp_server/FoodData_Central_csv_2024-10-31"
    
    # File to store logs
    log_file_path = os.path.join(base_path, 'analysis_log.txt')
    
    # Open the log file
    with open(log_file_path, 'w') as log_file:
        # Redirect print to log file
        def log_print(*args, **kwargs):
            print(*args, **kwargs)
            print(*args, **kwargs, file=log_file)
        
        # Analyze database structure
        log_print("Analyzing database structure...")
        analysis_results = analyze_database_structure(base_path)
        
        # Analyze nutrient mappings
        log_print("\nAnalyzing nutrient mappings...")
        nutrient_analysis = analyze_nutrient_mappings(base_path)
        
        # Validate data integrity
        log_print("\nValidating data integrity...")
        data_validation = validate_data_integrity(base_path)
        
        # Generate nutrient mapping
        log_print("\nGenerating nutrient mapping...")
        mapping_analysis = generate_nutrient_mapping(base_path)
        
        if 'error' not in mapping_analysis:
            log_print("\nNutrient Mapping Analysis:")
            log_print("=" * 50)
            log_print(f"\nTotal nutrients categorized: {mapping_analysis['stats']['categorized']}")
            
            for category, count in mapping_analysis['stats']['by_category'].items():
                log_print(f"\n{category.title()}:")
                log_print(f"- Count: {count}")
                if count > 0:
                    nutrients = mapping_analysis['raw_mapping'][category]
                    log_print("- Sample nutrients:")
                    for n in nutrients[:5]:
                        log_print(f"  * {n['name']} (ID: {n['id']}, Unit: {n['unit']})")
            
            # Save mapping to a separate file
            mapping_file = 'nutrient_mapping.json'
            with open(mapping_file, 'w') as f:
                json.dump(mapping_analysis['raw_mapping'], f, indent=2)
            log_print(f"\nDetailed nutrient mapping saved to '{mapping_file}'")
        
        # Save results to a JSON file
        output = deep_convert_dict({
            'table_analysis': analysis_results,
            'nutrient_analysis': nutrient_analysis,
            'data_validation': data_validation
        })
        
        with open('detailed_food_db_analysis.json', 'w') as f:
            json.dump(output, f, indent=2)
        
        # Print key findings
        log_print("\nKey Findings:")
        log_print("=" * 50)
        
        if 'error' not in nutrient_analysis:
            log_print("\nNutrient Analysis Details:")
            log_print("=" * 50)
            log_print(f"\nNutrient Counts:")
            log_print(f"- Total unique nutrients defined: {nutrient_analysis['nutrient_counts']['total_nutrients']}")
            log_print(f"- Total nutrient records: {nutrient_analysis['nutrient_counts']['total_food_nutrient_records']}")
            log_print(f"- Unique nutrient IDs in food-nutrient mapping: {nutrient_analysis['nutrient_counts']['unique_nutrient_ids_in_mapping']}")
            
            log_print("\nNutrient Units Distribution:")
            for unit, count in nutrient_analysis['nutrient_details']['nutrients_by_unit'].items():
                log_print(f"- {unit}: {count} nutrients")
            
            log_print("\nTop 10 Most Frequently Occurring Nutrients:")
            for nutrient_id, count in list(nutrient_analysis['nutrient_details']['top_occurring_nutrients'].items())[:10]:
                nutrient_info = next((n for n in nutrient_analysis['nutrient_details']['all_nutrients'] 
                                    if n['id'] == nutrient_id), {'name': f'Unknown ({nutrient_id})'})
                log_print(f"- {nutrient_info['name']}: {count} occurrences")
            
            log_print("\nUnmapped Nutrient Analysis:")
            log_print(f"- Nutrients in mapping but not defined: {len(nutrient_analysis['unmapped_nutrient_ids']['in_mapping_not_in_nutrients'])}")
            if nutrient_analysis['unmapped_nutrient_ids']['in_mapping_not_in_nutrients']:
                log_print("  First 5 unmapped IDs:", nutrient_analysis['unmapped_nutrient_ids']['in_mapping_not_in_nutrients'][:5])
            
            log_print(f"- Defined nutrients not in mapping: {len(nutrient_analysis['unmapped_nutrient_ids']['in_nutrients_not_in_mapping'])}")
            if nutrient_analysis['unmapped_nutrient_ids']['in_nutrients_not_in_mapping']:
                log_print("  First 5 unused nutrient IDs:", nutrient_analysis['unmapped_nutrient_ids']['in_nutrients_not_in_mapping'][:5])
        
        if 'error' not in data_validation:
            log_print("\nData Validation:")
            log_print(f"- Total foods: {data_validation['food_counts']['total_foods']}")
            log_print(f"- Foods with nutrients: {data_validation['food_counts']['foods_with_nutrients']}")
            log_print(f"- Average nutrients per food: {data_validation['nutrient_coverage']['avg_nutrients_per_food']:.2f}")
            log_print(f"- Foods missing nutrients: {data_validation['nutrient_coverage']['foods_missing_nutrients']}")
        
        log_print("\nDetailed analysis has been saved to 'detailed_food_db_analysis.json'")

if __name__ == "__main__":
    main()


