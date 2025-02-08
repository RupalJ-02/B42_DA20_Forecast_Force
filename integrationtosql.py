import pandas as pd
from sqlalchemy import create_engine

# Database Configuration
database_user = 'rupaljha'
database_password = 'root'
database_host = '127.0.0.1'
database_name = 'indiangithubdb'

# Create the database engine
engine = create_engine(f"mysql+mysqlconnector://{database_user}:{database_password}@{database_host}/{database_name}")
print("Database engine created successfully!")

# GitHub Indian Users dataset
github_file_path = 'Cleaned Better Schema Github Indian Users Deep Data.csv'
try:
    github_df = pd.read_csv(github_file_path)
except Exception as e:
    print(f"Error loading GitHub dataset: {e}")

# Manual Data Processing
try:
    # Handle missing values in 'Gender Pronoun'
    github_df['Gender Pronoun'].fillna('Unknown', inplace=True)

    # Convert Social Links to string type
    github_df['Social Links'] = github_df['Social Links'].astype(str)

except Exception as e:
    print(f"Error processing GitHub dataset: {e}")

# Upload GitHub dataset to SQL
github_table_name = 'github_users'
try:
    github_df.to_sql(github_table_name, con=engine, if_exists='replace', index=False)
    print(f"{github_table_name} is uploaded into SQL in {database_name} database")
except Exception as e:
    print(f"Error uploading GitHub dataset: {e}")