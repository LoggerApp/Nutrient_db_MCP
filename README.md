# USDA MCP Server

A high-performance Message Control Protocol (MCP) server implementation for accessing USDA nutritional data through Claude Desktop or other MCP-compatible clients.

[![Python Version](https://img.shields.io/badge/python-3.10%2B-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

## Features

- 🚀 Fast, optimized SQLite database for USDA nutritional data
- 🔍 Advanced food and nutrient search capabilities
- 📊 Nutrient density scoring and analysis
- 💪 Support for complex nutritional queries
- 🔄 Built-in caching for improved performance
- 📱 Claude Desktop integration

## Prerequisites

- Python 3.10 or higher
- SQLite 3.x
- Claude Desktop (latest version)
- `uv` package manager (recommended)

## Quick Start

1. **Clone the repository:**
   ```bash
   git clone https://github.com/yourusername/usda-mcp-server.git
   cd usda-mcp-server
   ```

2. **Set up environment:**
   ```bash
   # Create and activate virtual environment
   python -m venv venv
   source venv/bin/activate  # Unix
   # OR
   venv\Scripts\activate     # Windows
   
   # Install dependencies
   pip install -r requirements.txt
   ```

3. **Download USDA data:**
   - Visit [FoodData Central](https://fdc.nal.usda.gov/download-datasets.html)
   - Download "Full Download of All Data Types" (CSV format)
   - Extract files to `data/FoodData_Central_csv` directory

4. **Initialize database:**
   ```bash
   python setup_nutrient_db.py
   ```

5. **Configure Claude Desktop:**
   
   Edit `claude_desktop_config.json`:
   ```json
   {
     "mcpServers": {
       "usda": {
         "command": "usda-mcp-server",
         "args": ["--db-path", "/path/to/usda.db"]
       }
     }
   }
   ```

6. **Start using with Claude Desktop!**

## Database Setup Details

The database initialization process:

1. Downloads and processes USDA nutritional data
2. Creates optimized SQLite schema
3. Builds indexes for common query patterns
4. Calculates nutrient density scores
5. Generates summary statistics
6. Expect it to compile for multiple hours on most hardware

See [Setup Guide: USDA Nutrient Database.md](docs/Setup%20Guide:%20USDA%20Nutrient%20Database.md) for detailed instructions.

## Usage Examples

Ask Claude about nutrition:

```
What nutrients are in raw spinach?
```

Compare foods:
```
Compare the protein content of chicken breast, tofu, and black beans.
```

Find nutrient-dense foods:
```
What foods are highest in vitamin C and fiber?
```

## Available Resources

The server exposes several resources:

- `usda://foods/list` - Searchable food database
- `usda://nutrients/list` - Available nutrients
- `usda://foods/{food_id}` - Detailed food information
- `usda://foods/nutrient_dense` - Foods ranked by nutrient density

## Development

### Project Structure

```
usda-mcp-server/
├── src/
│   └── usda_mcp_server/
│       ├── server.py      # MCP server implementation
│       ├── schema.sql     # Database schema
│       └── tools/         # Utility scripts
├── tests/                 # Test suite
├── docs/                  # Documentation
└── data/                  # Data directory
```

### Running Tests

```bash
pytest tests/
```

### Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

## Performance Optimization

The server includes several optimizations:

- Efficient SQLite schema design
- Strategic indexing
- Query result caching
- Materialized views for common queries
- WAL journal mode

## Troubleshooting

See [Troubleshooting Guide](docs/troubleshooting.md) for common issues and solutions.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Acknowledgments

- USDA FoodData Central for the comprehensive nutrient database
- Claude Desktop team for the MCP protocol specification
- Contributors and maintainers

## Citation

If you use this project in your research, please cite:

```bibtex
@software{usda_mcp_server,
  title = {USDA MCP Server},
  url = {https://github.com/yourusername/usda-mcp-server},
  version = {0.2.0},
  year = {2024}
}
```
