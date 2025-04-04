from typing import Dict, List, Any, Optional
import json
import os

class SchemaManager:
    """Manages database schema context for tourism data"""
    
    def __init__(self):
        """Initialize schema manager"""
        self._initialize_schema_context()
    
    def _initialize_schema_context(self):
        """Initialize the schema context with tourism data structure"""
        self.schema_context = {
            "domain_knowledge": {
                "regions": {
                    "description": "The canton of Ticino is split into four tourism regions, including Bellinzonese",
                    "details": "Data is available at different geographic levels: canton, tourism regions, and smaller geographical tiles"
                },
                "visitor_categories": {
                    "swiss_commuters": "Swiss SIM cards, on the way to/from work zip code",
                    "swiss_locals": "Swiss SIM cards living in local zip codes",
                    "swiss_tourists": "Swiss SIM cards visiting (non-commuters and non-domestic)",
                    "foreign_workers": "Foreign SIM cards commuting to/from work",
                    "foreign_tourists": "Foreign SIM cards classified as visiting (non-commuters)"
                },
                "overnight_definition": "SIM cards spending at least 4 hours between 0:00 and 05:00 in the region",
                "dwell_time_buckets": ["0.5-1h", "1-2h", "2-3h", "3-4h", "4-5h", "5-6h", "6-7h", "7-8h", "8-24h"],
                "age_groups": ["0-19 years", "20-39 years", "40-64 years", "over 64 years"]
            },
            "tables": {
                "aoi_days_raw": {
                    "description": "Daily visitor data from Swisscom Tourism API for Areas of Interest",
                    "columns": {
                        "id": {"type": "integer", "description": "Primary key"},
                        "aoi_date": {"type": "date", "description": "Date of the visitor data"},
                        "aoi_id": {"type": "varchar(100)", "description": "Unique identifier for the area of interest"},
                        "visitors": {
                            "type": "jsonb",
                            "description": "Visitor counts by category (>30 min dwell time)",
                            "json_structure": {
                                "swissLocal": "Number of Swiss residents in local zip codes",
                                "swissTourist": "Number of Swiss visitors (non-commuters, non-local)",
                                "foreignWorker": "Number of foreign commuters",
                                "swissCommuter": "Number of Swiss commuters",
                                "foreignTourist": "Number of foreign visitors (non-commuters)"
                            }
                        },
                        "dwelltimes": {
                            "type": "jsonb",
                            "description": "Tourist dwell time distribution in hourly buckets",
                            "json_structure": "Array of counts for buckets: 0.5-1h, 1-2h, 2-3h, 3-4h, 4-5h, 5-6h, 6-7h, 7-8h, 8-24h"
                        },
                        "demographics": {
                            "type": "jsonb",
                            "description": "Demographic information about Swiss tourists",
                            "json_structure": {
                                "maleProportion": "Ratio of male visitors (float)",
                                "ageDistribution": "Array of 4 floats for age groups: 0-19, 20-39, 40-64, 65+"
                            }
                        },
                        "overnights_from_yesterday": {
                            "type": "jsonb",
                            "description": "Information about visitors who stayed overnight (4+ hours between 00:00-05:00)",
                            "json_structure": "Origin information of overnight visitors"
                        },
                        "top_foreign_countries": {
                            "type": "jsonb",
                            "description": "Top countries of origin for foreign tourists",
                            "json_structure": "Array of objects with country names and visitor counts"
                        },
                        "top_last_cantons": {
                            "type": "jsonb",
                            "description": "Previous day's canton origins (20+ min stay before arrival)",
                            "json_structure": "Array of canton names and counts"
                        },
                        "top_last_municipalities": {
                            "type": "jsonb",
                            "description": "Previous day's municipality origins (20+ min stay before arrival)",
                            "json_structure": "Array of municipality names and counts"
                        },
                        "top_swiss_cantons": {
                            "type": "jsonb",
                            "description": "Home cantons of Swiss tourists",
                            "json_structure": "Array of canton names and visitor counts"
                        },
                        "top_swiss_municipalities": {
                            "type": "jsonb",
                            "description": "Home municipalities of Swiss tourists",
                            "json_structure": "Array of municipality names and visitor counts"
                        }
                    }
                },
                "master_card": {
                    "description": "Mastercard Geo Insights data for 1.2x1.2km tiles, indexed to 2018 baseline",
                    "columns": {
                        "id": {"type": "integer", "description": "Primary key"},
                        "yr": {"type": "integer", "description": "Year of transaction"},
                        "txn_date": {"type": "date", "description": "Date of transaction"},
                        "industry": {"type": "varchar(255)", "description": "Industry sector"},
                        "segment": {"type": "varchar(255)", "description": "Market segment (overall, domestic, international)"},
                        "txn_amt": {"type": "numeric", "description": "Indexed total spend in the area"},
                        "txn_cnt": {"type": "numeric", "description": "Indexed total number of transactions"},
                        "acct_cnt": {"type": "numeric", "description": "Indexed number of distinct cards"},
                        "avg_ticket": {"type": "numeric", "description": "Average spend per transaction"},
                        "avg_freq": {"type": "numeric", "description": "Average transactions per card"},
                        "avg_spend": {"type": "numeric", "description": "Average spend per card"},
                        "geo_type": {"type": "varchar(255)", "description": "Geographic type"},
                        "geo_name": {"type": "varchar(255)", "description": "Geographic location name"},
                        "central_latitude": {"type": "numeric", "description": "Tile center latitude"},
                        "central_longitude": {"type": "numeric", "description": "Tile center longitude"}
                    }
                }
            },
            "relationships": [
                {
                    "description": "Geographic relationship between visitor data and spending patterns",
                    "tables": ["aoi_days_raw", "master_card"],
                    "join_condition": "Spatial join using geographic coordinates and names"
                }
            ],
            "query_patterns": {
                "visitor_analysis": {
                    "description": "Analyze visitor patterns and demographics",
                    "example": "SELECT aoi_date, (visitors->>'swissLocal')::numeric as locals, (visitors->>'swissTourist')::numeric as tourists FROM aoi_days_raw"
                },
                "overnight_analysis": {
                    "description": "Analyze overnight visitor patterns",
                    "example": "SELECT aoi_date, overnights_from_yesterday FROM aoi_days_raw WHERE overnights_from_yesterday IS NOT NULL"
                },
                "spending_patterns": {
                    "description": "Analyze spending patterns by location",
                    "example": "SELECT geo_name, industry, AVG(txn_amt) as avg_spend FROM master_card GROUP BY geo_name, industry"
                },
                "combined_analysis": {
                    "description": "Combined visitor and spending analysis",
                    "example": """
                    SELECT 
                        a.aoi_date,
                        (a.visitors->>'swissTourist')::numeric as tourists,
                        m.txn_amt as spending
                    FROM aoi_days_raw a
                    JOIN master_card m ON 
                        a.aoi_date = m.txn_date AND 
                        ST_DWithin(
                            ST_MakePoint(m.central_longitude, m.central_latitude),
                            ST_MakePoint(a.longitude, a.latitude),
                            0.01
                        )
                    """
                }
            }
        }
    
    def get_relevant_context(self, query: str, n_results: int = 5) -> Dict[str, Any]:
        """Get relevant schema context for a given query"""
        # Simple keyword-based context retrieval
        query = query.lower()
        context = {
            "domain_knowledge": {},
            "tables": {},
            "json_fields": {},
            "query_patterns": [],
            "description": []
        }
        
        # Add domain knowledge based on keywords
        for category, info in self.schema_context["domain_knowledge"].items():
            if any(keyword in query for keyword in [category, *str(info).lower().split()]):
                if isinstance(info, dict):
                    context["domain_knowledge"][category] = [info.get("description", ""), info.get("details", "")]
                else:
                    context["domain_knowledge"][category] = [str(info)]
        
        # Add table information based on keywords
        for table_name, table_info in self.schema_context["tables"].items():
            if table_name in query or table_info["description"].lower() in query:
                context["tables"][table_name] = {
                    "description": table_info["description"],
                    "columns": []
                }
                for col_name, col_info in table_info["columns"].items():
                    if col_name in query or col_info["description"].lower() in query:
                        if col_info.get("json_structure"):
                            key = f"{table_name}.{col_name}"
                            context["json_fields"][key] = []
                            if isinstance(col_info["json_structure"], dict):
                                for field, desc in col_info["json_structure"].items():
                                    context["json_fields"][key].append(f"{field}: {desc}")
                            else:
                                context["json_fields"][key].append(col_info["json_structure"])
                        context["tables"][table_name]["columns"].append(
                            f"{col_name} ({col_info['type']}): {col_info['description']}"
                        )
        
        # Add query patterns based on keywords
        for pattern_name, pattern_info in self.schema_context["query_patterns"].items():
            if pattern_name in query or pattern_info["description"].lower() in query:
                context["query_patterns"].append(
                    f"Query pattern {pattern_name}: {pattern_info['description']}\nExample: {pattern_info['example']}"
                )
        
        # Add all relevant descriptions
        context["description"] = [
            *[desc for info_list in context["domain_knowledge"].values() for desc in info_list],
            *[info["description"] for info in context["tables"].values()],
            *[col for table in context["tables"].values() for col in table.get("columns", [])],
            *[field for fields in context["json_fields"].values() for field in fields],
            *context["query_patterns"]
        ]
        
        return context

    def get_json_field_info(self, table: str, column: str) -> Optional[Dict]:
        """Get detailed information about a JSON field"""
        table_info = self.schema_context["tables"].get(table)
        if not table_info:
            return None
        
        column_info = table_info["columns"].get(column)
        if not column_info or "json_structure" not in column_info:
            return None
        
        return {
            "structure": column_info["json_structure"],
            "description": column_info["description"]
        }

# Initialize schema manager
schema_manager = SchemaManager() 