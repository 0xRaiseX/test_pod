import psycopg2
from mysql.connector import connect as mysql_connect
from pymongo import MongoClient
from datetime import datetime
from urllib.parse import urlparse

def test_database_connectivity(postgres_url: str, mysql_url: str, mongodb_url: str) -> dict:
    """
    Test connectivity, write, and read operations for PostgreSQL, MySQL, and MongoDB.
    Args:
        postgres_url: Connection string for PostgreSQL
        mysql_url: Connection string for MySQL
        mongodb_url: Connection string for MongoDB
    Returns:
        Dictionary with test results for each database
    """
    results = {
        'postgresql': {'connection': False, 'write': False, 'read': False, 'error': None},
        'mysql': {'connection': False, 'write': False, 'read': False, 'error': None},
        'mongodb': {'connection': False, 'write': False, 'read': False, 'error': None}
    }

    # Test data
    test_data = {
        'name': 'test_user',
        'timestamp': datetime.now().isoformat(),
        'value': 42
    }

    # PostgreSQL Test
    try:
        pg_conn = psycopg2.connect(postgres_url)
        results['postgresql']['connection'] = True
        pg_cursor = pg_conn.cursor()
        
        # Create table
        pg_cursor.execute("""
            CREATE TABLE IF NOT EXISTS test_table (
                id SERIAL PRIMARY KEY,
                name VARCHAR(100),
                timestamp VARCHAR(50),
                value INTEGER
            )
        """)
        
        # Write data
        pg_cursor.execute(
            "INSERT INTO test_table (name, timestamp, value) VALUES (%s, %s, %s) RETURNING id",
            (test_data['name'], test_data['timestamp'], test_data['value'])
        )
        results['postgresql']['write'] = True
        
        # Read data
        pg_cursor.execute("SELECT name, timestamp, value FROM test_table WHERE name = %s", (test_data['name'],))
        read_data = pg_cursor.fetchone()
        if read_data and read_data[0] == test_data['name']:
            results['postgresql']['read'] = True
            
        pg_conn.commit()
        pg_cursor.close()
        pg_conn.close()
        
    except Exception as e:
        results['postgresql']['error'] = str(e)

    # MySQL Test
    try:
        parsed_url = urlparse(mysql_url)
        if parsed_url.scheme != 'mysql':
            raise ValueError("Invalid MySQL URL scheme. Expected 'mysql://'")

        # Extract connection parameters
        config = {
            'host': parsed_url.hostname or 'localhost',
            'port': parsed_url.port or 3306,
            'user': parsed_url.username,
            'password': parsed_url.password,
            'database': parsed_url.path.lstrip('/')
        }

        # Connect to MySQL
        mysql_conn = mysql_connect(**config)
        results['mysql']['connection'] = True
        mysql_cursor = mysql_conn.cursor()

        # Create table
        mysql_cursor.execute("""
            CREATE TABLE IF NOT EXISTS test_table (
                id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(100),
                timestamp VARCHAR(50),
                value INT
            )
        """)

        # Write data
        mysql_cursor.execute(
            "INSERT INTO test_table (name, timestamp, value) VALUES (%s, %s, %s)",
            (test_data['name'], test_data['timestamp'], test_data['value'])
        )
        results['mysql']['write'] = True

        # Read data
        mysql_cursor.execute("SELECT name, timestamp, value FROM test_table WHERE name = %s", (test_data['name'],))
        read_data = mysql_cursor.fetchone()
        if read_data and read_data[0] == test_data['name']:
            results['mysql']['read'] = True

        mysql_conn.commit()
        mysql_cursor.close()
        mysql_conn.close()
    except Exception as e:
        results['mysql']['error'] = str(e)

    # MongoDB Test
    try:
        mongo_client = MongoClient(mongodb_url)
        mongo_db = mongo_client.test_database
        results['mongodb']['connection'] = True
        
        # Write data
        collection = mongo_db.test_collection
        insert_result = collection.insert_one(test_data)
        results['mongodb']['write'] = True
        
        # Read data
        read_data = collection.find_one({'_id': insert_result.inserted_id})
        if read_data and read_data['name'] == test_data['name']:
            results['mongodb']['read'] = True
            
        mongo_client.close()
        
    except Exception as e:
        results['mongodb']['error'] = str(e)

    return results

# Example usage:
# postgres_url = "postgresql://user:password@localhost:5432/testdb"
postgres_url = "postgresql://admin:WcFnjJdx@vd4a810ee3b1-db/app" 
# mysql_url = "host=localhost;user=root;password=password;database=testdb"
mysql_url = "mysql://admin:gowDeWXw@j2e59293926c-db/app"
# mongodb_url = "mongodb://localhost:27017/"
mongodb_url = "mongodb://admin:aHlWu5UF@f58dd222870d-db/app?authSource=admin"
results = test_database_connectivity(postgres_url, mysql_url, mongodb_url)
print(results)