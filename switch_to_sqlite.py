import os
import yaml
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("logs/switch_to_sqlite.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("switch_to_sqlite")

def switch_to_sqlite():
    """
    Switch the database configuration from Baserow to SQLite
    
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        # Load configuration
        config_path = "config/config.yaml"
        with open(config_path, 'r') as file:
            config = yaml.safe_load(file)
        
        # Update database type
        config['database']['type'] = 'sqlite'
        
        # Save configuration
        with open(config_path, 'w') as file:
            yaml.dump(config, file, default_flow_style=False)
        
        # Update .env file if it exists
        env_path = ".env"
        if os.path.exists(env_path):
            with open(env_path, 'r') as file:
                lines = file.readlines()
            
            # Update DB_TYPE line or add it if it doesn't exist
            db_type_found = False
            for i, line in enumerate(lines):
                if line.startswith('DB_TYPE='):
                    lines[i] = 'DB_TYPE=sqlite\n'
                    db_type_found = True
                    break
            
            if not db_type_found:
                lines.append('DB_TYPE=sqlite\n')
            
            # Write updated .env file
            with open(env_path, 'w') as file:
                file.writelines(lines)
        else:
            # Create .env file
            with open(env_path, 'w') as file:
                file.write('DB_TYPE=sqlite\n')
        
        logger.info("Successfully switched database configuration to SQLite")
        return True
    
    except Exception as e:
        logger.error(f"Error switching to SQLite: {str(e)}")
        return False

if __name__ == "__main__":
    if switch_to_sqlite():
        print("Successfully switched to SQLite database")
    else:
        print("Error switching to SQLite database")
