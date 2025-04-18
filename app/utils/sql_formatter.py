import logging
import re

logger = logging.getLogger(__name__)

def format_sql(query: str) -> str:
    """Format the SQL query with proper indentation and line breaks"""
    try:
        # Simple formatting - split on keywords and rejoin with proper spacing
        # Add keywords that should start a new line
        keywords = ["SELECT", "FROM", "WHERE", "GROUP BY", "ORDER BY", "LIMIT", "WITH", "UNION", "INNER JOIN", "LEFT JOIN", "RIGHT JOIN", "ON"]
        
        # Normalize whitespace and split lines
        query = re.sub(r'\s+', ' ', query).strip()
        
        formatted_query = ""
        indent_level = 0
        lines = query.split(' ')
        
        for i, word in enumerate(lines):
            upper_word = word.upper()
            
            # Check if word is a keyword that should start a new line
            if upper_word in keywords and i > 0: 
                # Handle JOIN ON specifically
                if upper_word == "ON" and lines[i-1].upper() == "JOIN":
                    formatted_query += f" {word}"
                else:
                     # Decrease indent before keywords like FROM, WHERE etc. if not SELECT/WITH
                    if upper_word not in ["SELECT", "WITH"] and indent_level > 0:
                         # This basic logic might need refinement for complex queries
                         pass # Simple formatter doesn't handle complex indentation well
                    formatted_query += f"\n{'  ' * indent_level}{word}" 
                    # Increase indent after SELECT/WITH 
                    if upper_word in ["SELECT", "WITH"]:
                        indent_level +=1 # Basic indenting
            elif upper_word.endswith(',') and upper_word not in ["SELECT", "FROM"]: # Break after comma in select/group by
                 formatted_query += f" {word}\n{'  ' * indent_level}"
            elif upper_word == "AND" or upper_word == "OR": # New line for AND/OR in WHERE
                 formatted_query += f"\n{'  ' * (indent_level+1)}{word}" # Indent under WHERE
            else:
                # Append word with a space if not the beginning of the formatted string
                formatted_query += f"{' ' if formatted_query else ''}{word}"

        return formatted_query.strip()

    except Exception as e:
        logger.error(f"Error formatting query: {str(e)}")
        return query  # Return original query if formatting fails 