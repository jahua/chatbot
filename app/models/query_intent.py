from enum import Enum

class QueryIntent(Enum):
    VISITOR_COUNT = "visitor_count"
    VISITOR_COMPARISON = "visitor_comparison"
    SPENDING_ANALYSIS = "spending_analysis"
    CORRELATION_ANALYSIS = "correlation_analysis"
    PEAK_PERIOD = "peak_period"
    TREND_ANALYSIS = "trend_analysis"
    HOTSPOT_DETECTION = "hotspot_detection"
    REGION_ANALYSIS = "region_analysis"
    SPATIAL_PATTERN = "spatial_pattern"
    GEO_SPATIAL = "geo_spatial" 