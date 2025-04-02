import google.generativeai as genai
import os

def test_gemini():
    api_key = "AIzaSyA-hZV7NlvVXqUhvYHyc8tGj612kgmyY1g"  # Using the working API key
    print(f"Using API key: {api_key[:5]}...")
    
    genai.configure(api_key=api_key)
    model = genai.GenerativeModel('gemini-1.5-pro')  # Using a stable model from the available list
    
    schema_context = """Database Schema:

Table: data_lake.aoi_days_raw
  - id: integer
  - aoi_date: date
  - aoi_id: character varying
  - visitors: jsonb
  - dwelltimes: jsonb
  - demographics: jsonb
  - overnights_from_yesterday: jsonb
  - top_foreign_countries: jsonb
  - top_last_cantons: jsonb
  - top_last_municipalities: jsonb
  - top_swiss_cantons: jsonb
  - top_swiss_municipalities: jsonb
  - source_system: character varying
  - load_date: date
  - ingestion_timestamp: timestamp without time zone
  - raw_content: jsonb"""

    prompt = f"""You are a SQL expert. Generate a PostgreSQL query for the following user query.

Schema Context:
{schema_context}

User Query:
Show me all regions

Return ONLY the SQL query, nothing else."""
    
    try:
        generation_config = genai.types.GenerationConfig(
            temperature=0.2,
            top_p=0.8,
            top_k=40,
            max_output_tokens=1024,
        )
        
        safety_settings = [
            {
                "category": "HARM_CATEGORY_HARASSMENT",
                "threshold": "BLOCK_NONE",
            },
            {
                "category": "HARM_CATEGORY_HATE_SPEECH",
                "threshold": "BLOCK_NONE",
            },
            {
                "category": "HARM_CATEGORY_SEXUALLY_EXPLICIT",
                "threshold": "BLOCK_NONE",
            },
            {
                "category": "HARM_CATEGORY_DANGEROUS_CONTENT",
                "threshold": "BLOCK_NONE",
            },
        ]
        
        print("\nSending prompt:", prompt)
        response = model.generate_content(
            prompt,
            generation_config=generation_config,
            safety_settings=safety_settings
        )
        print("\nResponse:", response.text)
    except Exception as e:
        print("Error:", str(e))

if __name__ == "__main__":
    test_gemini() 